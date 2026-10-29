use crate::common::{self, Config};
use fs2::FileExt;
use rusqlite::params;
use std::cmp::Ordering;
use std::fs::{self, OpenOptions};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::time::Duration;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};


#[derive(Clone, Debug)]
pub struct QueueItem {
    pub tier: i32,
    pub size: i64,
    pub path: String,
}

impl PartialEq for QueueItem {
    fn eq(&self, other: &Self) -> bool {
        self.tier == other.tier && self.size == other.size
    }
}
impl Eq for QueueItem {}
impl PartialOrd for QueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for QueueItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.tier.cmp(&other.tier) {
            Ordering::Equal => other.size.cmp(&self.size),
            ord => ord,
        }
    }
}

#[derive(Clone)]
pub struct DispatcherHandle {
    pub queue: Arc<Mutex<Vec<QueueItem>>>,
    pub condvar: Arc<Condvar>,
}

// --- COMPACT TRACKING TOKEN GENERATOR ---
fn encode_tracking_token(original_path: &str) -> String {
    let mut hasher = DefaultHasher::new();
    original_path.hash(&mut hasher);
    let hash_val = hasher.finish();
    format!("{:x}", hash_val) // Generates a short, 16-character hex hash unique to the path
}

// --------------------------------------

pub fn run(config: &Arc<RwLock<Config>>) -> DispatcherHandle {
    let db_path = {
        let cfg = config.read().unwrap();
        cfg.database_path
            .as_deref()
            .filter(|p| !p.is_empty())
            .expect("❌ [DB ERROR] 'database_path' is not defined in config.json!")
            .to_string()
    };
        
    let lock_file_path = format!("{}.lock", db_path);

    println!("🚀 [DISPATCH] Initializing event-driven dispatcher module...");

    if let Ok(mut conn) = common::open_database(&db_path) {
        common::init_database(&mut conn);
    }

    let queue = Arc::new(Mutex::new(Vec::new()));
    let condvar = Arc::new(Condvar::new());

    {
        let lock_file = OpenOptions::new().read(true).write(true).create(true).open(&lock_file_path).ok();
        if let Some(ref lf) = lock_file { let _ = lf.lock_exclusive(); }
        
        if let Ok(conn) = common::open_database(&db_path) {
            if let Ok(mut stmt) = conn.prepare("SELECT tier, size, path FROM media_files WHERE status = 'queued' ORDER BY tier ASC, size ASC") {
                if let Ok(rows) = stmt.query_map([], |row| {
                    Ok(QueueItem {
                        tier: row.get(0)?,
                        size: row.get(1)?,
                        path: row.get(2)?,
                    })
                }) {
                    let mut q = queue.lock().unwrap();
                    for item in rows.flatten() {
                        if Path::new(&item.path).exists() {
                            q.push(item);
                        }
                    }
                    q.sort();
                    println!("📥 [QUEUE PRE-LOAD] Loaded {} active queued items into memory.", q.len());
                    condvar.notify_all();
                }
            }
        }
        if let Some(lf) = lock_file { let _ = lf.unlock(); }
    }

    let dispatcher_handle = DispatcherHandle {
        queue: Arc::clone(&queue),
        condvar: Arc::clone(&condvar),
    };

    let initial_servers = {
        let cfg = config.read().unwrap();
        cfg.servers.clone()
    };

    for server_config in initial_servers {
        let db_p = db_path.to_string();
        let lock_p = lock_file_path.clone();
        let srv = server_config.clone();
        let disp_handle_clone = dispatcher_handle.clone();

        thread::spawn(move || {
            server_completion_loop(&db_p, &lock_p, &srv, &disp_handle_clone);
        });
    }

    let q_clone = Arc::clone(&queue);
    let cv_clone = Arc::clone(&condvar);
    let cfg_clone = Arc::clone(config);
    let db_p = db_path.to_string();
    let lock_p = lock_file_path.clone();

    thread::spawn(move || {
        dispatcher_main_loop(&db_p, &lock_p, &cfg_clone, q_clone, &cv_clone);
    });

    dispatcher_handle
}

fn count_valid_items(dir_path: &str, allowed_exts: &[String]) -> usize {
    let path = Path::new(dir_path);
    if !path.exists() || !path.is_dir() { return 0; }
    let mut count = 0;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_file() {
                let filename = p.file_name().unwrap_or_default().to_string_lossy();
                if filename.ends_with(".tmp") || filename.contains("_converted") || filename.contains("_raw_tmp") {
                    continue;
                }
                if let Some(ext) = p.extension().and_then(|e| e.to_str()) {
                    let ext_lower = format!(".{}", ext.to_lowercase());
                    if allowed_exts.iter().any(|e| e.eq_ignore_ascii_case(&ext_lower)) { 
                        count += 1; 
                    }
                }
            }
        }
    }
    count
}

fn get_server_load(server: &serde_json::Value, allowed_exts: &[String]) -> usize {
    let a_path = server.get("path_2a").and_then(|v| v.as_str()).unwrap_or("");
    let b_path = server.get("path_2b").and_then(|v| v.as_str()).unwrap_or("");

    let mut count = 0;
    if !b_path.is_empty() { count += count_valid_items(b_path, allowed_exts); }
    if a_path != b_path && !a_path.is_empty() { count += count_valid_items(a_path, allowed_exts); }
    count
}

fn select_available_server<'a>(cfg: &'a Config) -> Option<serde_json::Value> {
    let servers = &cfg.servers;
    if servers.is_empty() { return None; }

    // Pass 1: Strict Numerical Priority for completely idle servers (load == 0)
    // Ensures Server 1 gets preference, but overflows to Server 2 if Server 1 has even 1 file and Server 2 is empty.
    for server in servers {
        let max_q = server.get("max_queue").and_then(|v| v.as_i64()).unwrap_or(2) as usize;
        let load = get_server_load(server, &cfg.allowed_extensions);
        if load == 0 && max_q > 0 {
            let srv_id = server.get("id").and_then(|v| v.as_str()).unwrap_or("server_1");
            println!("⚖️ [LOAD BALANCER] Assigned to {} (Strict Priority Idle, Load: {}/{})", srv_id, load, max_q);
            return Some(server.clone());
        }
    }

    // Pass 2: Fallback - If all servers have at least 1 file, find the first numerically prioritized server with capacity
    for server in servers {
        let max_q = server.get("max_queue").and_then(|v| v.as_i64()).unwrap_or(2) as usize;
        let load = get_server_load(server, &cfg.allowed_extensions);
        if load < max_q {
            let srv_id = server.get("id").and_then(|v| v.as_str()).unwrap_or("server_1");
            println!("⚖️ [LOAD BALANCER] Assigned to {} (Strict Priority Fallback, Load: {}/{})", srv_id, load, max_q);
            return Some(server.clone());
        }
    }

    None
}

fn copy_item_to_destination(source_path: &str, dest_path: &str, server_id: &str) -> bool {
    let dest_dir = Path::new(dest_path).parent();
    if let Some(dir) = dest_dir { 
        if let Err(e) = fs::create_dir_all(dir) {
            eprintln!("❌ [TRANSFER ERROR] Failed to create destination directory '{}': {}", dir.display(), e);
            return false;
        }
    }
    let tmp_dest = format!("{}.tmp", dest_path);

    for attempt in 1..=3 {
        if !Path::new(source_path).exists() {
            eprintln!("❌ [TRANSFER ERROR] Source file does not exist on disk: {}", source_path);
            return false;
        }
        let _ = fs::remove_file(&tmp_dest);
        let _ = fs::remove_file(dest_path);

        match fs::copy(source_path, &tmp_dest) {
            Ok(bytes_copied) => {
                match fs::rename(&tmp_dest, dest_path) {
                    Ok(_) => {
                        println!("✅ [TRANSFER COMPLETE] Successfully transferred {} bytes to [{}] -> {}", bytes_copied, server_id, dest_path);
                        return true;
                    }
                    Err(e) => {
                        eprintln!("❌ [TRANSFER ERROR] Failed to rename .tmp file to final destination on attempt {}: {}", attempt, e);
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ [TRANSFER ERROR] Failed to copy source file on attempt {}: {}", attempt, e);
                let _ = fs::remove_file(&tmp_dest);
                thread::sleep(Duration::from_secs(attempt * 2));
            }
        }
    }
    false
}

fn dispatcher_main_loop(
    db_path: &str,
    lock_file_path: &str,
    config: &Arc<RwLock<Config>>,
    queue: Arc<Mutex<Vec<QueueItem>>>,
    condvar: &Arc<Condvar>,
) {
    loop {
        let mut q = queue.lock().unwrap();
        while q.is_empty() {
            q = condvar.wait(q).unwrap();
        }

        let task = q.remove(0);
        drop(q);

        if !Path::new(&task.path).exists() {
            let _ = update_db_status(db_path, lock_file_path, &task.path, "missing");
            continue;
        }

        // --- BULLETPROOF DISPATCHER GUARD ---
        let (is_still_queued, current_status) = {
            let lock_file = match OpenOptions::new().read(true).write(true).create(true).open(lock_file_path) {
                Ok(f) => f,
                Err(_) => continue,
            };
            let _ = lock_file.lock_exclusive();
            let conn = match common::open_database(db_path) {
                Ok(c) => c,
                Err(_) => {
                    let _ = lock_file.unlock();
                    continue;
                }
            };
            let status: Option<String> = conn.query_row(
                "SELECT status FROM media_files WHERE path = ?1",
                params![task.path],
                |row| row.get(0)
            ).ok().flatten();
            let _ = lock_file.unlock();
            
            let is_queued = status.as_deref() == Some("queued");
            (is_queued, status.unwrap_or_else(|| "unknown".to_string()))
        };

        if !is_still_queued {
            println!("🛡️ [DISPATCH GUARD] Skipping re-dispatch for '{}' (Current DB status: '{}')", task.path, current_status);
            continue; 
        }
        // ----------------------------------

        let target_server = {
            let cfg = config.read().unwrap();
            match select_available_server(&cfg) {
                Some(s) => s,
                None => {
                    drop(cfg);
                    let mut q = queue.lock().unwrap();
                    q.insert(0, task);
                    drop(q);
                    thread::sleep(Duration::from_secs(5));
                    continue;
                }
            }
        };

        let srv_id = target_server.get("id").and_then(|v| v.as_str()).unwrap_or("server_1");
        let path_2a = target_server.get("path_2a").and_then(|v| v.as_str()).unwrap_or("");
        
        let path_obj = Path::new(&task.path);
        let full_stem = path_obj.file_stem().unwrap_or_default().to_string_lossy();
        let ext = path_obj.extension().and_then(|e| e.to_str()).unwrap_or("mkv");
        let token = encode_tracking_token(&task.path);
        
        let safe_stem = if full_stem.len() > 40 {
            &full_stem[..40]
        } else {
            &full_stem
        };
        
        let dispatch_filename = format!("{}__{}__{}.{}", safe_stem, srv_id, token, ext);
        let final_dest = format!("{}/{}", path_2a, dispatch_filename);

        println!("🚀 [DISPATCH] Sending item '{}' (Tier {}, Size: {} bytes) to server [{}]", safe_stem, task.tier, task.size, srv_id);

        let dispatch_status = if copy_item_to_destination(&task.path, &final_dest, srv_id) {
            format!("dispatched_{}", srv_id)
        } else {
            "dispatch_failed".to_string()
        };

        let _ = update_db_status(db_path, lock_file_path, &task.path, &dispatch_status);
    }
}

fn update_db_status(db_path: &str, lock_file_path: &str, path: &str, status: &str) -> Result<(), ()> {
    let lock_file = OpenOptions::new().read(true).write(true).create(true).open(lock_file_path).map_err(|_| ())?;
    let _ = lock_file.lock_exclusive();
    let conn = common::open_database(db_path).map_err(|_| { let _ = lock_file.unlock(); })?;
    let _ = conn.execute("UPDATE media_files SET status = ?1, updated_at = ?2 WHERE path = ?3", params![status, chrono::Utc::now().timestamp(), path]);
    let _ = lock_file.unlock();
    Ok(())
}

fn server_completion_loop(db_path: &str, lock_file_path: &str, server: &serde_json::Value, dispatcher_handle: &DispatcherHandle) {
    let srv_id = server.get("id").and_then(|v| v.as_str()).unwrap_or("server_x");
    let c_path = server.get("path_2c").and_then(|v| v.as_str()).unwrap_or("");
    let f_path = server.get("path_failed").and_then(|v| v.as_str()).unwrap_or("");
    let unmatched_dir = server.get("path_unmatched").and_then(|v| v.as_str()).unwrap_or("");

    let db_p1 = db_path.to_string();
    let db_p2 = db_path.to_string();
    let lock_p1 = lock_file_path.to_string();
    let lock_p2 = lock_file_path.to_string();
    let srv_id_clone = srv_id.to_string();
    let unmatched_clone = unmatched_dir.to_string();
    let disp_handle_c = dispatcher_handle.clone();
    let disp_handle_f = dispatcher_handle.clone();

    if !c_path.is_empty() {
        let c_path_str = c_path.to_string();
        let srv_id_c = srv_id.to_string();
        thread::spawn(move || {
            let path_obj = Path::new(&c_path_str);
            loop {
                thread::sleep(Duration::from_secs(10));
                if !path_obj.exists() || !path_obj.is_dir() { continue; }

                if let Ok(entries) = fs::read_dir(path_obj) {
                    for entry in entries.flatten() {
                        let p = entry.path();
                        if p.is_file() {
                            let filename = p.file_name().unwrap_or_default().to_string_lossy().to_string();
                            if filename.ends_with(".tmp") { continue; }

                            let source_str = p.to_string_lossy().to_string();
                            println!("📥 [SERVER PICKUP] Detected completed item on [{}]: '{}'", srv_id_c, filename);

                            let original_path = match parse_original_path_from_filename(&filename, &db_p1, &lock_p1, &srv_id_c) {
                                Some(op) => op,
                                None => {
                                    if !unmatched_clone.is_empty() {
                                        let _ = fs::create_dir_all(&unmatched_clone);
                                        let unmatched_dest = format!("{}/{}", unmatched_clone, filename);
                                        if fs::rename(&source_str, &unmatched_dest).is_ok() {
                                            let info_filepath = format!("{}.unmatched.txt", unmatched_dest);
                                            let info_content = format!(
                                                "UNMATCHED / REJECTED MEDIA REPORT\n==================================\nFilename: {}\nServer ID: {}\nRejection Reason: Token missing or database fallback failed\nTimestamp: {}\n",
                                                filename, srv_id_c, chrono::Utc::now()
                                            );
                                            let _ = fs::write(&info_filepath, info_content);
                                        }
                                        println!("⚠️ [UNMATCHED] Moved unknown completed file to unmatched directory with report: {}", filename);
                                    }
                                    continue;
                                }
                            };

                            let mut rejection_reason = None;
                            let mut expected_mtime_val = 0;

                            if let Ok(expected_mtime) = get_db_mtime(&db_p1, &lock_p1, &original_path) {
                                expected_mtime_val = expected_mtime;
                                if Path::new(&original_path).exists() {
                                    if let Ok(meta) = fs::metadata(&original_path) {
                                        let current_mtime = meta.modified()
                                            .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64)
                                            .unwrap_or(0);
                                        
                                        if current_mtime != expected_mtime {
                                            rejection_reason = Some(format!("mtime_mismatch (Expected: {}, Found: {})", expected_mtime, current_mtime));
                                        }
                                    } else {
                                        rejection_reason = Some("metadata_read_failure".to_string());
                                    }
                                } else {
                                    rejection_reason = Some("original_file_missing".to_string());
                                }
                            } else {
                                rejection_reason = Some("db_record_not_found".to_string());
                            }

                            if let Some(reason) = rejection_reason {
                                let human_reason = match reason.as_str() {
                                    r if r.starts_with("mtime_mismatch") => "The original file on disk was modified or replaced while the server was processing it.",
                                    "original_file_missing" => "The original source file no longer exists on disk.",
                                    "metadata_read_failure" => "Failed to read file metadata for the original path.",
                                    "db_record_not_found" => "No matching database record could be found for this file.",
                                    _ => "An unknown validation error occurred.",
                                };

                                if !unmatched_clone.is_empty() {
                                    let _ = fs::create_dir_all(&unmatched_clone);
                                    let unmatched_dest = format!("{}/{}", unmatched_clone, filename);
                                    
                                    if fs::rename(&source_str, &unmatched_dest).is_ok() {
                                        let info_filepath = format!("{}.unmatched.txt", unmatched_dest);
                                        let info_content = format!(
                                            "UNMATCHED / REJECTED MEDIA REPORT\n==================================\nFilename: {}\nServer ID: {}\nOriginal Path: {}\nHuman Reason: {}\nTechnical Reason: {}\nExpected Mtime: {}\nTimestamp: {}\n",
                                            filename, srv_id_c, original_path, human_reason, reason, expected_mtime_val, chrono::Utc::now()
                                        );
                                        let _ = fs::write(&info_filepath, info_content);
                                        println!("⚠️ [UNMATCHED] File rejected. Reason: {} [Technical: {}] -> Filename: {}", human_reason, reason, filename);
                                    }
                                }
                                continue;
                            }

                            let orig_parent = Path::new(&original_path).parent().unwrap_or(Path::new("")).to_string_lossy();
                            let orig_stem = Path::new(&original_path).file_stem().unwrap_or_default().to_string_lossy();
                            let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("mkv");
                            
                            let correct_dest = format!("{}/{}.{}", orig_parent, orig_stem, ext);

                            if copy_item_to_destination(&source_str, &correct_dest, &srv_id_c) {
                                if let Ok(meta) = fs::metadata(&correct_dest) {
                                    let new_size = meta.len() as i64;
                                    let new_mtime = meta.modified().map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64).unwrap_or(0);

                                    let _ = finalize_completion_in_db(&db_p1, &lock_p1, &original_path, &correct_dest, new_size, new_mtime);
                                    let _ = fs::remove_file(&source_str);
                                    println!("🎉 [FINALIZE COMPLETE] Restored to original name and marked 'completed': {}.{}", orig_stem, ext);
                                    disp_handle_c.condvar.notify_all();
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    if !f_path.is_empty() {
        let f_path_str = f_path.to_string();
        let unmatched_clone_f = unmatched_dir.to_string();
        thread::spawn(move || {
            let path_obj = Path::new(&f_path_str);
            loop {
                thread::sleep(Duration::from_secs(10));
                if !path_obj.exists() || !path_obj.is_dir() { continue; }

                if let Ok(entries) = fs::read_dir(path_obj) {
                    for entry in entries.flatten() {
                        let p = entry.path();
                        if p.is_file() {
                            let filename = p.file_name().unwrap_or_default().to_string_lossy().to_string();
                            if filename.ends_with(".tmp") { continue; }

                            let source_str = p.to_string_lossy().to_string();
                            println!("❌ [SERVER FAILURE] Detected failed item on [{}]: '{}'", srv_id_clone, filename);

                            let original_path = match parse_original_path_from_filename(&filename, &db_p2, &lock_p2, &srv_id_clone) {
                                Some(op) => op,
                                None => {
                                    if !unmatched_clone_f.is_empty() {
                                        let _ = fs::create_dir_all(&unmatched_clone_f);
                                        let unmatched_dest = format!("{}/{}", unmatched_clone_f, filename);
                                        
                                        let fallback_path = get_original_path_fallback(&db_p2, &lock_p2, &filename);
                                        let human_reason = "The tracking token was missing from the filename or could not be matched to any active database record.";

                                        if fs::rename(&source_str, &unmatched_dest).is_ok() {
                                            let info_filepath = format!("{}.unmatched.txt", unmatched_dest);
                                            let info_content = format!(
                                                "UNMATCHED / REJECTED MEDIA REPORT\n==================================\nFilename: {}\nServer ID: {}\nOriginal Path (Fallback Lookup): {}\nHuman Reason: {}\nTechnical Reason: Token missing or database fallback failed\nTimestamp: {}\n",
                                                filename, srv_id_clone, fallback_path, human_reason, chrono::Utc::now()
                                            );
                                            let _ = fs::write(&info_filepath, info_content);
                                        }
                                        println!("⚠️ [UNMATCHED] Failed file unmatched. Reason: {} [Technical: Token missing/unparseable] -> Filename: {}", human_reason, filename);
                                    }
                                    continue;
                                }
                            };

                            let _ = update_db_status(&db_p2, &lock_p2, &original_path, "failed");
                            let _ = fs::remove_file(&source_str);
                            println!("🔴 [DB UPDATE] Marked item as 'failed' in database: {}", filename);
                            disp_handle_f.condvar.notify_all();
                        }
                    }
                }
            }
        });
    }
}

fn parse_original_path_from_filename(filename: &str, db_path: &str, lock_file_path: &str, srv_id: &str) -> Option<String> {
    get_original_path_from_db(db_path, lock_file_path, filename, srv_id)
}

fn get_original_path_from_db(db_path: &str, lock_file_path: &str, filename: &str, srv_id: &str) -> Option<String> {
    let lock_file = OpenOptions::new().read(true).write(true).create(true).open(lock_file_path).ok()?;
    let _ = lock_file.lock_exclusive();
    let conn = common::open_database(db_path).ok()?;
    let target_dispatch_status = format!("dispatched_{}", srv_id);

    let parts: Vec<&str> = filename.split("__").collect();
    if parts.is_empty() {
        let _ = lock_file.unlock();
        return None;
    }
    let stem_prefix = parts[0];
    let stem_pattern = format!("{}%.%", stem_prefix);

    let mut stmt = conn.prepare(
        "SELECT path FROM media_files WHERE (filename = ?1 OR filename LIKE ?2) AND (status = ?3 OR status = 'queued' OR status LIKE 'dispatched_%')"
    ).ok()?;
    
    let res: Option<String> = stmt.query_row(params![filename, stem_pattern, target_dispatch_status], |row| row.get(0)).ok();
    let _ = lock_file.unlock();
    res
}

fn get_db_mtime(db_path: &str, lock_file_path: &str, path: &str) -> Result<i64, ()> {
    let lock_file = OpenOptions::new().read(true).write(true).create(true).open(lock_file_path).map_err(|_| ())?;
    let _ = lock_file.lock_exclusive();
    let conn = common::open_database(db_path).map_err(|_| { let _ = lock_file.unlock(); })?;
    
    let mut stmt = conn.prepare("SELECT mtime FROM media_files WHERE path = ?1").map_err(|_| { let _ = lock_file.unlock(); })?;
    let mtime: i64 = stmt.query_row(params![path], |row| row.get(0)).map_err(|_| { let _ = lock_file.unlock(); })?;
    
    let _ = lock_file.unlock();
    Ok(mtime)
}

fn finalize_completion_in_db(db_path: &str, lock_file_path: &str, original_path: &str, correct_dest: &str, size: i64, mtime: i64) -> Result<(), ()> {
    let lock_file = OpenOptions::new().read(true).write(true).create(true).open(lock_file_path).map_err(|_| ())?;
    let _ = lock_file.lock_exclusive();
    let mut conn = common::open_database(db_path).map_err(|_| { let _ = lock_file.unlock(); })?;
    let tx = conn.transaction().map_err(|_| { let _ = lock_file.unlock(); })?;

    if original_path != correct_dest && Path::new(original_path).exists() {
        let _ = tx.execute("DELETE FROM media_files WHERE path = ?1", params![original_path]);
        let _ = fs::remove_file(original_path);
    }

    let filename = Path::new(correct_dest).file_name().unwrap_or_default().to_string_lossy().to_string();
    
    let processed_tags = match crate::common::load_config("config.json") {
        Ok(cfg) => cfg.processed_tags,
        Err(e) => {
            eprintln!("❌ [CONFIG ERROR] {}. Using empty processed tags for ffprobe.", e);
            vec![]
        }
    };
    
    let (v_codec, resolution, audio_ch, tag) = crate::common::run_ffprobe(correct_dest, &processed_tags);

    let _ = tx.execute(
        "INSERT INTO media_files (path, filename, size, mtime, video_codec, resolution, audio_channels, tag, status, updated_at) 
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'completed', ?9)
         ON CONFLICT(path) DO UPDATE SET 
            size = ?3, mtime = ?4, video_codec = ?5, resolution = ?6, audio_channels = ?7, tag = ?8, status = 'completed', updated_at = ?9",
        params![correct_dest, filename, size, mtime, v_codec, resolution, audio_ch, tag, chrono::Utc::now().timestamp()],
    );

    let _ = tx.commit();
    let _ = lock_file.unlock();

    let completed_path = correct_dest.to_string();
    thread::spawn(move || {
        let _ = std::process::Command::new("python3")
            .arg("arr_sync_service.py")
            .arg(&completed_path)
            .spawn();
    });

    Ok(())
}

fn get_original_path_fallback(db_path: &str, lock_file_path: &str, filename: &str) -> String {
    let lock_file = match OpenOptions::new().read(true).write(true).create(true).open(lock_file_path) {
        Ok(f) => f,
        Err(_) => return "DB Lock Failed".to_string(),
    };
    let _ = lock_file.lock_exclusive();
    let conn = match common::open_database(db_path) {
        Ok(c) => c,
        Err(_) => {
            let _ = lock_file.unlock();
            return "DB Open Failed".to_string();
        }
    };

    let stem = filename.split("__").next().unwrap_or(filename);
    let pattern = format!("%{}%", stem);

    let mut stmt = match conn.prepare("SELECT path FROM media_files WHERE filename LIKE ?1 OR path LIKE ?1 LIMIT 1") {
        Ok(s) => s,
        Err(_) => {
            let _ = lock_file.unlock();
            return "Query Prepare Failed".to_string();
        }
    };

    let result: Option<String> = stmt.query_row(params![pattern], |row| row.get(0)).ok();
    let _ = lock_file.unlock();

    result.unwrap_or_else(|| "Not found in database".to_string())
}