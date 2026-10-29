use crate::common::{self, Config, FileDiskInfo, ProbedMedia, file_passes_filters, run_ffprobe, process_and_clean_srt_disk, get_applicable_filter_config};
use crate::dispatch::{self, QueueItem};
use fs2::FileExt;
use rusqlite::{params, Connection};
use std::path::Path;
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex, Condvar};
use std::fs::OpenOptions;

pub fn run(config: &Arc<RwLock<Config>>, dispatcher_handle: &dispatch::DispatcherHandle) {
    let db_path = {
        let cfg = config.read().unwrap();
        cfg.database_path
            .as_deref()
            .filter(|p| !p.is_empty())
            .expect("❌ [DB ERROR] 'database_path' is not defined in config.json!")
            .to_string()
    };

    println!("🗄️ [DATABASE] Opening connection to: {}", db_path);
    let mut conn = common::open_database(&db_path).unwrap_or_else(|e| {
        panic!("❌ [DB ERROR] Failed to open SQLite database at '{}': {}", db_path, e)
    });

    common::init_database(&mut conn);

    run_unified_scan(&mut conn, config, dispatcher_handle);
}

fn run_unified_scan(conn: &mut Connection, config: &Arc<RwLock<Config>>, dispatcher_handle: &dispatch::DispatcherHandle) {
    let db_path = {
        let cfg = config.read().unwrap();
        cfg.database_path.clone().unwrap()
    };
    let lock_file_path = format!("{}.lock", db_path);

    let (source_paths, srt_clean_terms) = {
        let cfg = config.read().unwrap();
        (
            cfg.global_source_paths.clone(),
            cfg.srt_clean_terms.iter().map(|t| t.to_lowercase()).collect::<Vec<String>>(),
        )
    };
    let source_path_strs: Vec<&str> = source_paths.iter().map(|s| s.as_str()).collect();

    println!("🦀 [RUST SCAN] Starting multi-threaded disk walk via Rayon...");
    
    let all_files: Vec<FileDiskInfo> = source_path_strs.par_iter().flat_map(|sp| {
        let path = Path::new(sp);
        if !path.exists() {
            return vec![];
        }
        walkdir::WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .filter_map(|e| {
                let p = e.path();
                let p_str = p.display().to_string();
                let filename = p.file_name()?.to_str()?.to_string();
                let filename_lower = filename.to_lowercase();

                if filename_lower.ends_with(".tmp") || filename_lower.contains("_converted") || filename_lower.contains("_raw_tmp") {
                    return None;
                }

                let is_srt = filename_lower.ends_with(".srt");
                let cfg = config.read().unwrap();
                let fc = get_applicable_filter_config(&p_str, &cfg);
                let allowed_exts = if !fc.allowed_extensions.is_empty() { &fc.allowed_extensions } else { &cfg.allowed_extensions };

                let is_media = allowed_exts.iter().any(|ext| {
                    let ext_lower = ext.to_lowercase();
                    filename_lower.ends_with(&ext_lower) || filename_lower.ends_with(&format!(".{}", ext_lower.trim_start_matches('.')))
                });

                if is_srt || is_media {
                    let mut status_hint = None;
                    if is_media {
                        let matches_skip = fc.skip_terms.iter().any(|term| filename_lower.contains(&term.to_lowercase())) ||
                                           cfg.skip_terms.iter().any(|term| filename_lower.contains(&term.to_lowercase()));
                        if matches_skip { status_hint = Some("skip_term".to_string()); }
                    }
                    drop(cfg);

                    if let Ok(meta) = std::fs::metadata(p) {
                        let size = meta.len() as i64;
                        let mtime = meta.modified().map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64).unwrap_or(0);
                        return Some(FileDiskInfo { path: p_str, filename, size, mtime, status_hint });
                    }
                }
                None
            })
            .collect::<Vec<FileDiskInfo>>()
    }).collect();

    let (srt_files, disk_files): (Vec<FileDiskInfo>, Vec<FileDiskInfo>) = all_files.into_par_iter().partition(|f| {
        f.path.to_lowercase().ends_with(".srt")
    });

    println!("🦀 [RUST SCAN] Walked {} valid media files and {} SRT files.", disk_files.len(), srt_files.len());

    let mut scanned_disk_paths: HashMap<String, FileDiskInfo> = HashMap::new();
    for f in &srt_files { scanned_disk_paths.insert(f.path.clone(), f.clone()); }
    for f in &disk_files { scanned_disk_paths.insert(f.path.clone(), f.clone()); }

    let mut db_records: HashMap<String, (i64, i64, Option<String>, Option<String>, Option<String>, Option<String>, String)> = HashMap::new();
    {
        let mut stmt = conn.prepare("SELECT path, size, mtime, video_codec, resolution, audio_channels, tag, status FROM media_files").unwrap();
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, String>(7)?,
            ))
        }).unwrap();

        for row in rows {
            if let Ok((path, size, mtime, v_codec, res, audio, tag, status)) = row {
                db_records.insert(path, (size, mtime, v_codec, res, audio, tag, status));
            }
        }
    }

    let mut missing_count = 0;
    for (db_path, (_size, _mtime, _v_codec, _res, _audio, _tag, db_status)) in &db_records {
        if db_status != "missing" && !db_status.starts_with("dispatched_server_") && db_status != "completed" && db_status != "processing" {
            let matches_active_path = source_path_strs.iter().any(|sp| db_path.starts_with(sp));
            if !matches_active_path || !scanned_disk_paths.contains_key(db_path) {
                process_and_save_single_item(
                    &db_path,
                    &lock_file_path,
                    &ProbedMedia {
                        path: db_path.clone(),
                        filename: Path::new(db_path).file_name().unwrap_or_default().to_string_lossy().to_string(),
                        size: 0,
                        mtime: 0,
                        video_codec: None,
                        resolution: None,
                        audio_channels: None,
                        tag: None,
                        status: "missing".to_string(),
                    },
                    &dispatcher_handle.queue,
                    &dispatcher_handle.condvar,
                );
                missing_count += 1;
            }
        }
    }

    let srt_to_process: Vec<FileDiskInfo> = srt_files.into_iter().filter(|srt| {
        match db_records.get(&srt.path) {
            Some(&(db_size, db_mtime, _, _, _, _, ref db_status)) => {
                let is_dispatched_or_completed = db_status.starts_with("dispatched_server_") || db_status == "completed" || db_status == "processing";
                let size_time_changed = srt.size != db_size || srt.mtime != db_mtime;
                if is_dispatched_or_completed && !size_time_changed { return false; }
                srt.size != db_size || srt.mtime != db_mtime || db_status == "missing" || db_status == "failed" || is_dispatched_or_completed
            }
            None => true,
        }
    }).collect();

    let cleaned_srt_results: Vec<(String, String, i64, i64)> = srt_to_process.into_par_iter().filter_map(|srt| {
        process_and_clean_srt_disk(&srt.path, &srt_clean_terms)
    }).collect();

    for (path, filename, size, mtime) in cleaned_srt_results {
        process_and_save_single_item(
            &db_path,
            &lock_file_path,
            &ProbedMedia {
                path,
                filename,
                size,
                mtime,
                video_codec: None,
                resolution: None,
                audio_channels: None,
                tag: Some("SRT_Cleaned".to_string()),
                status: "srt_cleaned".to_string(),
            },
            &dispatcher_handle.queue,
            &dispatcher_handle.condvar,
        );
    }

    let mut files_to_reuse = Vec::new();
    let mut files_to_probe = Vec::new();

    for df in disk_files {
        let is_skip_term = df.status_hint.as_deref() == Some("skip_term");
        match db_records.get(&df.path) {
            Some(&(db_size, db_mtime, ref v_codec, ref res, ref audio, ref tag, ref db_status)) => {
                let is_dispatched_or_completed = db_status.starts_with("dispatched_server_") || db_status == "completed" || db_status == "processing";
                let size_time_changed = df.size != db_size || df.mtime != db_mtime;

                if is_dispatched_or_completed && !size_time_changed {
                    continue;
                }

                let has_blank_metadata = v_codec.is_none();
                let was_missing = db_status == "missing";
                let was_failed = db_status == "failed";

                if is_skip_term {
                    files_to_reuse.push(ProbedMedia {
                        path: df.path, filename: df.filename, size: df.size, mtime: df.mtime,
                        video_codec: None, resolution: None, audio_channels: None, tag: None,
                        status: "skip_term".to_string(),
                    });
                } else if size_time_changed || has_blank_metadata || was_missing || was_failed || is_dispatched_or_completed {
                    files_to_probe.push(df);
                } else {
                    let cfg = config.read().unwrap();
                    let (passes, _) = file_passes_filters(&df.path, df.size, df.mtime, v_codec.as_deref(), res.as_deref(), audio.as_deref(), tag.as_deref(), &cfg);
                    drop(cfg);
                    let status = if passes { "queued".to_string() } else { "filtered_out".to_string() };
                    
                    files_to_reuse.push(ProbedMedia {
                        path: df.path, filename: df.filename, size: df.size, mtime: df.mtime,
                        video_codec: v_codec.clone(), resolution: res.clone(), audio_channels: audio.clone(), tag: tag.clone(),
                        status,
                    });
                }
            }
            None => { files_to_probe.push(df); }
        }
    }

    for item in files_to_reuse {
        process_and_save_single_item(&db_path, &lock_file_path, &item, &dispatcher_handle.queue, &dispatcher_handle.condvar);
    }

    println!("🦀 [RUST SCAN] Running parallel ffprobe on {} files with 1-by-1 DB updates...", files_to_probe.len());

    files_to_probe.into_par_iter().for_each(|df| {
        let item = if df.status_hint.as_deref() == Some("skip_term") {
            ProbedMedia {
                path: df.path, filename: df.filename, size: df.size, mtime: df.mtime,
                video_codec: None, resolution: None, audio_channels: None, tag: None,
                status: "skip_term".to_string(),
            }
        } else {
            let processed_tags = {
                let cfg = config.read().unwrap();
                cfg.processed_tags.clone()
            };
            let (v_codec, resolution, audio_channels, tag) = run_ffprobe(&df.path, &processed_tags);
            let cfg = config.read().unwrap();
            let (passes, _) = file_passes_filters(&df.path, df.size, df.mtime, v_codec.as_deref(), resolution.as_deref(), audio_channels.as_deref(), tag.as_deref(), &cfg);
            drop(cfg);
            let status = if passes { "queued".to_string() } else { "filtered_out".to_string() };
            ProbedMedia {
                path: df.path, filename: df.filename, size: df.size, mtime: df.mtime,
                video_codec: v_codec, resolution, audio_channels, tag, status,
            }
        };

        process_and_save_single_item(&db_path, &lock_file_path, &item, &dispatcher_handle.queue, &dispatcher_handle.condvar);
    });

    println!("🦀 [RUST SCAN] Database synchronization complete. Marked missing: {}", missing_count);

    std::thread::spawn(move || {
        let _ = std::process::Command::new("python3").arg("arr_sync_service.py").spawn();
    });
}

fn process_and_save_single_item(
    db_path: &str,
    lock_file_path: &str,
    item: &ProbedMedia,
    queue: &Arc<Mutex<Vec<QueueItem>>>,
    condvar: &Arc<Condvar>,
) {
    let lock_file = match OpenOptions::new().read(true).write(true).create(true).open(lock_file_path) {
        Ok(f) => f,
        Err(_) => return,
    };

    if lock_file.lock_exclusive().is_err() {
        return;
    }

    let mut conn = match common::open_database(db_path) {
        Ok(c) => c,
        Err(_) => {
            let _ = lock_file.unlock();
            return;
        }
    };

    let tx = match conn.transaction() {
        Ok(t) => t,
        Err(_) => {
            let _ = lock_file.unlock();
            return;
        }
    };

    let _ = tx.execute(
        "INSERT INTO media_files (path, filename, size, mtime, video_codec, resolution, audio_channels, tag, status, updated_at) 
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(path) DO UPDATE SET 
            size = ?3, 
            mtime = ?4, 
            video_codec = COALESCE(?5, media_files.video_codec), 
            resolution = COALESCE(?6, media_files.resolution), 
            audio_channels = COALESCE(?7, media_files.audio_channels), 
            tag = COALESCE(?8, media_files.tag), 
            status = ?9,
            updated_at = ?10",
        params![
            item.path, item.filename, item.size, item.mtime,
            item.video_codec, item.resolution, item.audio_channels, item.tag,
            item.status, chrono::Utc::now().timestamp()
        ]
    );

    let _ = tx.commit();
    let _ = lock_file.unlock();

    if item.status == "queued" {
        let mut q = queue.lock().unwrap();
        q.push(QueueItem {
            tier: 2,
            size: item.size,
            path: item.path.clone(),
        });
        q.sort();
        drop(q);
        condvar.notify_one();
    }
}