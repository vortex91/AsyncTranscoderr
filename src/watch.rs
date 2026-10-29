use crate::common::{self, Config, file_passes_filters, run_ffprobe};
use crate::dispatch::{self, QueueItem};
use fs2::FileExt;
use notify::{Config as NotifyConfig, RecommendedWatcher, RecursiveMode, Result as NotifyResult, Watcher, Event, EventKind};
use rusqlite::params;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::path::Path;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, RwLock, Mutex, Condvar};
use std::thread;
use std::time::{Duration, Instant};
use std::io::{self, Write};

pub fn run_with_signal(config: &Arc<RwLock<Config>>, dispatcher_handle: &dispatch::DispatcherHandle, signal: Sender<()>) {
    let db_path = {
        let cfg = config.read().unwrap();
        cfg.database_path
            .as_deref()
            .filter(|p| !p.is_empty())
            .expect("❌ [DB ERROR] 'database_path' is not defined in config.json!")
            .to_string()
    };
    
    let lock_file_path = format!("{}.lock", db_path);

    println!("👀 [WATCH] Initializing real-time file system watcher (Event-driven kernel mode)...");
    let _ = io::stdout().flush();

    let (tx, rx) = channel::<NotifyResult<Event>>();
    let mut watcher = match RecommendedWatcher::new(tx, NotifyConfig::default()) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("❌ [WATCH ERROR] Failed to create file system watcher: {}", e);
            let _ = signal.send(());
            return;
        }
    };

    let source_paths = {
        let cfg = config.read().unwrap();
        cfg.global_source_paths.clone()
    };

    if source_paths.is_empty() {
        eprintln!("❌ [WATCH ERROR] source_paths is EMPTY! Check config.json for 'global_source_paths'.");
        let _ = signal.send(());
        return;
    }

    for sp in &source_paths {
        let path = Path::new(sp);
        println!("🔍 [WATCH] Registering watch on NFS path: {}", sp);
        let _ = io::stdout().flush();
        if path.exists() {
            watcher.watch(path, RecursiveMode::Recursive)
                .unwrap_or_else(|e| eprintln!("⚠️ [WATCH ERROR] Failed to watch path '{}': {}", sp, e));
        } else {
            eprintln!("⚠️ [WATCH WARNING] Path does not exist on disk: {}", sp);
        }
        let _ = io::stdout().flush();
    }

    let config_path = Path::new("config.json");
    if config_path.exists() {
        let _ = watcher.watch(config_path, RecursiveMode::NonRecursive);
    }

    println!("👀 [WATCH] Event-driven listener loop active and ready.");
    let _ = io::stdout().flush();

    let _ = signal.send(());

    let config_arc = Arc::clone(config);
    let db_path_clone = db_path.to_string();
    let lock_path_clone = lock_file_path.clone();
    
    let q_handle = Arc::clone(&dispatcher_handle.queue);
    let cv_handle = Arc::clone(&dispatcher_handle.condvar);

    thread::spawn(move || {
        let mut recent_events: HashMap<String, Instant> = HashMap::new();
        let debounce_window = Duration::from_secs(5);

        let _keep_alive = watcher;

        while let Ok(result) = rx.recv() {
            match result {
                Ok(event) => {
                    if matches!(event.kind, EventKind::Create(_) | EventKind::Modify(_) | EventKind::Other) {
                        for path in event.paths {
                            let p_str = path.display().to_string();

                            let now = Instant::now();
                            if let Some(last_time) = recent_events.get(&p_str) {
                                if now.duration_since(*last_time) < debounce_window {
                                    continue;
                                }
                            }
                            recent_events.insert(p_str.clone(), now);

                            if p_str.ends_with("config.json") {
                                println!("⚙️ [WATCH CONFIG] Changes detected in config.json. Hot-reloading...");
                                match common::load_config("config.json") {
                                    Ok(new_cfg) => {
                                        let mut cfg_lock = config_arc.write().unwrap();
                                        *cfg_lock = new_cfg;
                                        println!("✨ [WATCH CONFIG] Config hot-reloaded successfully into live memory!");
                                    }
                                    Err(e) => {
                                        eprintln!("❌ [CONFIG ERROR] {}. Keeping previous config.", e);
                                    }
                                }
                                continue;
                            }

                            let filename_lower = path.file_name().unwrap_or_default().to_string_lossy().to_lowercase();

                            if filename_lower.ends_with(".tmp") || 
                               filename_lower.ends_with(".part") ||
                               filename_lower.contains("_converted") || 
                               filename_lower.contains("_raw_tmp") {
                                continue;
                            }

                            let is_srt = filename_lower.ends_with(".srt");
                            
                            let is_media = {
                                let cfg = config_arc.read().unwrap();
                                let fc = common::get_applicable_filter_config(&p_str, &cfg);
                                let allowed_exts = if !fc.allowed_extensions.is_empty() {
                                    &fc.allowed_extensions
                                } else {
                                    &cfg.allowed_extensions
                                };

                                allowed_exts.iter().any(|ext| {
                                    let ext_lower = ext.to_lowercase();
                                    filename_lower.ends_with(&ext_lower) || filename_lower.ends_with(&format!(".{}", ext_lower.trim_start_matches('.')))
                                })
                            };

                            if is_srt || is_media {
                                println!("📥 [WATCH EVENT] Real-time file change detected: {}", path.file_name().unwrap_or_default().to_string_lossy());
                                
                                let db_p = db_path_clone.clone();
                                let lock_p = lock_path_clone.clone();
                                let cfg_clone = Arc::clone(&config_arc);
                                let file_path_clone = p_str.clone();
                                
                                let q_inner = Arc::clone(&q_handle);
                                let cv_inner = Arc::clone(&cv_handle);

                                thread::spawn(move || {
                                    thread::sleep(Duration::from_secs(3));
                                    let current_cfg = {
                                        let cfg = cfg_clone.read().unwrap();
                                        (*cfg).clone()
                                    };

                                    process_single_file_with_lock(
                                        &db_p,
                                        &lock_p,
                                        &file_path_clone,
                                        &current_cfg,
                                        &q_inner,
                                        &cv_inner,
                                    );
                                });
                            }
                        }
                    }
                }
                Err(e) => eprintln!("⚠️ [WATCH ERROR] Watcher notification error: {:?}", e),
            }
        }
    });
}

fn wait_for_file_stability(path_str: &str, max_attempts: usize, delay_secs: u64) -> bool {
    let path = Path::new(path_str);
    let mut last_size = -1;

    for _ in 0..max_attempts {
        if !path.exists() {
            return false;
        }
        if let Ok(meta) = fs::metadata(path) {
            let current_size = meta.len() as i64;
            if current_size == last_size && current_size > 0 {
                return true;
            }
            last_size = current_size;
        }
        thread::sleep(Duration::from_secs(delay_secs));
    }
    false
}

fn process_single_file_with_lock(
    db_path: &str,
    lock_file_path: &str,
    path_str: &str,
    config: &Config,
    queue: &Arc<Mutex<Vec<QueueItem>>>,
    condvar: &Arc<Condvar>,
) {
    let path_obj = Path::new(path_str);
    if !path_obj.exists() {
        return;
    }

    let lower = path_str.to_lowercase();
    let is_srt = lower.ends_with(".srt");

    if !is_srt {
        println!("⏳ [WATCH STABILITY] Waiting for file to stabilize on disk: {}", path_obj.file_name().unwrap_or_default().to_string_lossy());
        if !wait_for_file_stability(path_str, 6, 5) {
            println!("⚠️ [WATCH STABILITY] File failed to stabilize or was removed: {}", path_str);
            return;
        }
    }

    let filename_lower = path_obj.file_name().unwrap_or_default().to_string_lossy().to_lowercase();
    let fc = common::get_applicable_filter_config(path_str, config);
    let matches_skip = !is_srt && (fc.skip_terms.iter().any(|term| filename_lower.contains(&term.to_lowercase())) ||
                       config.skip_terms.iter().any(|term| filename_lower.contains(&term.to_lowercase())));

    let (v_codec, resolution, audio_channels, tag) = if is_srt || matches_skip {
        (None, None, None, None)
    } else {
        run_ffprobe(path_str, &config.processed_tags)
    };

    if !is_srt && !matches_skip && v_codec.is_none() {
        println!("⏳ [WATCH PROBE] ffprobe failed to extract video codec for '{}'. Skipping status update.", path_obj.file_name().unwrap_or_default().to_string_lossy());
        return;
    }

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

    let existing_status: Option<String> = conn.query_row(
        "SELECT status FROM media_files WHERE path = ?1",
        params![path_str],
        |row| row.get(0)
    ).ok().flatten();

    let is_dispatched_or_terminal = existing_status.as_ref().map_or(false, |st| {
        st == "completed" || st == "processing" || st.starts_with("dispatched_server_")
    });

    if is_dispatched_or_terminal {
        let _ = lock_file.unlock();
        return;
    }

    // Check if record already existed to determine if it's new or edited
    let existing_info: Option<(i64, i64)> = conn.query_row(
        "SELECT size, mtime FROM media_files WHERE path = ?1",
        params![path_str],
        |row| Ok((row.get(0)?, row.get(1)?))
    ).ok();

    let tx = match conn.transaction() {
        Ok(t) => t,
        Err(_) => {
            let _ = lock_file.unlock();
            return;
        }
    };

    let mut trigger_arr = false;

    if is_srt {
        let clean_terms: Vec<String> = config.srt_clean_terms.iter().map(|t| t.to_lowercase()).collect();
        if let Some((_, filename, size, mtime)) = common::process_and_clean_srt_disk(path_str, &clean_terms) {
            let _ = tx.execute(
                "INSERT INTO media_files (path, filename, size, mtime, tag, status, updated_at) 
                 VALUES (?1, ?2, ?3, ?4, 'SRT_Cleaned', 'srt_cleaned', ?5)
                 ON CONFLICT(path) DO UPDATE SET size = ?3, mtime = ?4, status = 'srt_cleaned', updated_at = ?5",
                params![path_str, filename, size, mtime, chrono::Utc::now().timestamp()],
            );
            println!("🧹 [WATCH SRT] Real-time subtitle cleaned and registered: {}", filename);
        }
    } else {
        if let Ok(meta) = fs::metadata(path_obj) {
            let size = meta.len() as i64;
            let mtime = meta.modified()
                .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64)
                .unwrap_or(0);
            let filename = path_obj.file_name().unwrap().to_str().unwrap().to_string();

            // Determine if row is newly added or edited on disk (size/mtime change)
            let is_new_or_edited = match existing_info {
                None => true,
                Some((db_size, db_mtime)) => size != db_size || mtime != db_mtime,
            };

            // If ffprobe ran successfully and it's new/edited (and not skipped), trigger Arr with path
            if !matches_skip && v_codec.is_some() && is_new_or_edited {
                trigger_arr = true;
            }

            let (passes, _) = file_passes_filters(
                path_str, size, mtime, v_codec.as_deref(), resolution.as_deref(), audio_channels.as_deref(), tag.as_deref(), config
            );

            let status = if matches_skip {
                "skip_term".to_string()
            } else if passes {
                "queued".to_string()
            } else {
                "filtered_out".to_string()
            };

            let _ = tx.execute(
                "INSERT INTO media_files (path, filename, size, mtime, video_codec, resolution, audio_channels, tag, status, updated_at) 
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(path) DO UPDATE SET 
                    size = ?3, mtime = ?4, 
                    video_codec = COALESCE(?5, media_files.video_codec), 
                    resolution = COALESCE(?6, media_files.resolution), 
                    audio_channels = COALESCE(?7, media_files.audio_channels), 
                    tag = COALESCE(?8, media_files.tag), 
                    status = ?9, updated_at = ?10",
                params![
                    path_str, filename, size, mtime,
                    v_codec, resolution, audio_channels, tag,
                    status, chrono::Utc::now().timestamp()
                ],
            );
            
            println!("✨ [WATCH SYNC] Real-time file processed: {}", filename);

            if status == "queued" {
                let mut q = queue.lock().unwrap();
                q.push(QueueItem {
                    tier: 2,
                    size,
                    path: path_str.to_string(),
                });
                q.sort();
                drop(q);
                condvar.notify_one();
            }
        }
    }

    let _ = tx.commit();
    let _ = lock_file.unlock();

    // Trigger arr_sync_service.py with path if a new/edited media file completed ffprobe
    if trigger_arr {
        let path_to_sync = path_str.to_string();
        thread::spawn(move || {
            let _ = std::process::Command::new("python3")
                .arg("arr_sync_service.py")
                .arg(&path_to_sync)
                .spawn();
        });
    }
}