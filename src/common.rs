use rusqlite::{Connection, params};
use fs2::FileExt;
use serde::Deserialize;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::Command;
use std::collections::HashSet;

#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum TimestampValue {
    String(String),
    Int(i64),
    Float(f64),
}

impl TimestampValue {
    pub fn to_unix_timestamp(&self) -> Option<i64> {
        match self {
            TimestampValue::Int(val) => Some(*val),
            TimestampValue::Float(val) => Some(*val as i64),
            TimestampValue::String(s) => {
                let s = s.trim();
                if s.is_empty() || s == "0" {
                    return Some(0);
                }
                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    return Some(dt.and_utc().timestamp());
                }
                if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    return Some(date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp());
                }
                if let Ok(f) = s.parse::<f64>() {
                    return Some(f as i64);
                }
                None
            }
        }
    }
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct FilterConfig {
    #[serde(default)]
    pub skip_if_tagged: Vec<String>,
    #[serde(default)]
    pub skip_codecs: Vec<String>,
    #[serde(default)]
    pub skip_terms: Vec<String>, 
    #[serde(default)]
    pub allowed_extensions: Vec<String>, 
    pub min_size_bytes: Option<i64>,
    pub max_size_bytes: Option<i64>,
    pub min_audio_channels: Option<i32>,
    pub min_height: Option<i32>,
    pub max_height: Option<i32>,
    pub min_width: Option<i32>,
    pub max_width: Option<i32>,
    pub modified_after: Option<TimestampValue>,
    pub modified_before: Option<TimestampValue>,
    pub modified_after_timestamp: Option<TimestampValue>,
    pub modified_before_timestamp: Option<TimestampValue>,
}

pub fn open_database(db_path: &str) -> rusqlite::Result<Connection> {
    let conn = Connection::open(db_path)?;
    let _ = conn.pragma_update(None, "journal_mode", "WAL");
    let _ = conn.pragma_update(None, "busy_timeout", 5000);
    Ok(conn)
}

pub fn init_database(conn: &mut Connection) {
    let _ = conn.execute(
        "CREATE TABLE IF NOT EXISTS media_files (
            path TEXT PRIMARY KEY,
            filename TEXT,
            size INTEGER,
            mtime INTEGER,
            video_codec TEXT,
            resolution TEXT,
            audio_channels TEXT,
            tag TEXT,
            status TEXT,
            updated_at INTEGER,
            radarr_quality_profile TEXT,
            sonarr_unmonitored INTEGER DEFAULT 0,
            tier INTEGER DEFAULT 2
        )",
        [],
    );

    ensure_column_exists(conn, "mtime", "INTEGER NOT NULL DEFAULT 0");
    ensure_column_exists(conn, "video_codec", "TEXT");
    ensure_column_exists(conn, "resolution", "TEXT");
    ensure_column_exists(conn, "audio_channels", "TEXT");
    ensure_column_exists(conn, "tag", "TEXT");
    ensure_column_exists(conn, "status", "TEXT NOT NULL DEFAULT 'queued'");
    ensure_column_exists(conn, "updated_at", "INTEGER NOT NULL DEFAULT 0");
    ensure_column_exists(conn, "radarr_quality_profile", "TEXT");
    ensure_column_exists(conn, "sonarr_unmonitored", "INTEGER DEFAULT 0");
    ensure_column_exists(conn, "tier", "INTEGER DEFAULT 2");
}

#[derive(Deserialize, Debug, Clone)]
pub struct PathFilterRule {
    pub paths: Vec<String>,
    pub filter_config: FilterConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub database_path: Option<String>,
    pub global_source_paths: Vec<String>,
    pub allowed_extensions: Vec<String>,
    #[serde(default)]
    pub skip_terms: Vec<String>,
    #[serde(default)]
    pub srt_clean_terms: Vec<String>,
    #[serde(default)]
    pub processed_tags: Vec<String>,
    #[serde(default)]
    pub servers: Vec<Value>,
    #[serde(default)]
    pub default_filter_config: FilterConfig,
    #[serde(default)]
    pub path_filter_rules: Vec<PathFilterRule>,
    #[serde(default)]
    pub scan_interval_hours: Option<u64>,
    #[serde(default)]
    pub scan_time_of_day: Option<String>,
}

#[derive(Clone)]
pub struct FileDiskInfo {
    pub path: String,
    pub filename: String,
    pub size: i64,
    pub mtime: i64,
    pub status_hint: Option<String>,
}

pub struct ProbedMedia {
    pub path: String,
    pub filename: String,
    pub size: i64,
    pub mtime: i64,
    pub video_codec: Option<String>,
    pub resolution: Option<String>,
    pub audio_channels: Option<String>,
    pub tag: Option<String>,
    pub status: String,
}

pub fn load_config(config_path: &str) -> Result<Config, String> {
    let content = std::fs::read_to_string(config_path)
        .map_err(|e| format!("Could not find or read configuration file at '{}': {}", config_path, e))?;
    
    let config: Config = serde_json::from_str(&content)
        .map_err(|e| format!("Failed to parse '{}': {}", config_path, e))?;

    println!("🦀 [CONFIG] Successfully loaded configuration from {}", config_path);
    Ok(config)
}

pub fn get_applicable_filter_config<'a>(path: &str, config: &'a Config) -> &'a FilterConfig {
    for rule in &config.path_filter_rules {
        for prefix in &rule.paths {
            if path.starts_with(prefix) {
                return &rule.filter_config;
            }
        }
    }
    
    println!("⚠️ [CONFIG WARNING] File path missed all path_filter_rules (check case-sensitivity!), falling back to default_filter_config: {}", path);
    &config.default_filter_config
}

pub fn file_passes_filters(path: &str, size: i64, mtime: i64, video_codec: Option<&str>, resolution: Option<&str>, audio_channels: Option<&str>, tag: Option<&str>, config: &Config) -> (bool, String) {
    let fc = get_applicable_filter_config(path, config);

    let filename = Path::new(path).file_name().unwrap_or_default().to_string_lossy();
    let filename_lower = filename.to_lowercase();
    for term in &fc.skip_terms {
        if filename_lower.contains(&term.to_lowercase()) {
            return (false, format!("Filename contains skip term '{}'", term));
        }
    }
    if let Some(t) = tag {
        if !fc.skip_if_tagged.is_empty() && fc.skip_if_tagged.iter().any(|st| t.to_lowercase().contains(&st.to_lowercase())) {
            return (false, format!("Already tagged ({})", t));
        }
    }

    if let Some(codec) = video_codec {
        if fc.skip_codecs.iter().any(|sc| sc.eq_ignore_ascii_case(codec)) {
            return (false, format!("Video codec '{}' is in skip list", codec));
        }
    }

    if let Some(min_s) = fc.min_size_bytes {
        if min_s > 0 && size < min_s {
            return (false, format!("File size {} below minimum threshold", size));
        }
    }

    if let Some(max_s) = fc.max_size_bytes {
        if size > max_s {
            return (false, format!("File size {} exceeds maximum threshold", size));
        }
    }

    if let Some(min_ch) = fc.min_audio_channels {
        let ch_int = audio_channels.and_then(|c| c.parse::<f64>().ok()).map(|v| v as i32).unwrap_or(0);
        if ch_int < min_ch {
            return (false, format!("Audio channels ({}) below minimum threshold ({})", ch_int, min_ch));
        }
    }

    let after_val = fc.modified_after.as_ref().or(fc.modified_after_timestamp.as_ref());
    if let Some(after_val) = after_val {
        if let Some(after_ts) = after_val.to_unix_timestamp() {
            if mtime < after_ts {
                return (false, format!("File modification time older than modified_after threshold"));
            }
        }
    }

    let before_val = fc.modified_before.as_ref().or(fc.modified_before_timestamp.as_ref());
    if let Some(before_val) = before_val {
        if let Some(before_ts) = before_val.to_unix_timestamp() {
            if mtime > before_ts {
                return (false, format!("File modification time newer than modified_before threshold"));
            }
        }
    }

    if let Some(res) = resolution {
        if res.contains('x') {
            let parts: Vec<&str> = res.split('x').collect();
            if parts.len() == 2 {
                if let (Ok(w), Ok(h)) = (parts[0].parse::<i32>(), parts[1].parse::<i32>()) {
                    let min_h = fc.min_height;
                    let max_h = fc.max_height;
                    let min_w = fc.min_width;
                    let max_w = fc.max_width;

                    let has_h_bounds = min_h.is_some() || max_h.is_some();
                    let has_w_bounds = min_w.is_some() || max_w.is_some();

                    if has_h_bounds || has_w_bounds {
                        let mut height_passes = true;
                        if let Some(mh) = min_h { if h < mh { height_passes = false; } }
                        if let Some(mxh) = max_h { if h > mxh { height_passes = false; } }

                        let mut width_passes = true;
                        if let Some(mw) = min_w { if w < mw { width_passes = false; } }
                        if let Some(mxw) = max_w { if w > mxw { width_passes = false; } }

                        let resolution_passed = if has_h_bounds && has_w_bounds {
                            height_passes || width_passes
                        } else if has_h_bounds {
                            height_passes
                        } else {
                            width_passes
                        };

                        if !resolution_passed {
                            return (false, format!("Resolution ({}x{}) does not match height OR width criteria", w, h));
                        }
                    }
                }
            }
        }
    }

    (true, "Passed all filters".to_string())
}

pub fn ensure_column_exists(conn: &mut Connection, col_name: &str, col_def: &str) {
    let query = format!("PRAGMA table_info(media_files)");
    let mut stmt = conn.prepare(&query).unwrap();
    let columns: Vec<String> = stmt.query_map([], |row| row.get(1))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();

    if !columns.contains(&col_name.to_string()) {
        println!("🗄️ [DATABASE MIGRATION] Adding missing column '{}' to media_files...", col_name);
        let alter_query = format!("ALTER TABLE media_files ADD COLUMN {} {}", col_name, col_def);
        conn.execute(&alter_query, []).expect(&format!("Failed to add column {}", col_name));
    }
}

pub fn process_and_clean_srt_disk(path: &str, clean_terms: &[String]) -> Option<(String, String, i64, i64)> {
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    let lines: Vec<String> = reader.lines().filter_map(|l| l.ok()).collect();

    let mut needs_cleaning = false;
    for line in &lines {
        let lower_line = line.to_lowercase();
        if clean_terms.iter().any(|term| lower_line.contains(term)) {
            needs_cleaning = true;
            break;
        }
    }

    if needs_cleaning {
        println!("🧹 [SRT CLEAN] Removing watermark terms from subtitle: {}", path);
        let tmp_path = format!("{}.tmp", path);
        if let Ok(mut tmp_file) = File::create(&tmp_path) {
            for line in &lines {
                let lower_line = line.to_lowercase();
                if !clean_terms.iter().any(|term| lower_line.contains(term)) {
                    let _ = writeln!(tmp_file, "{}", line);
                }
            }
            let _ = std::fs::rename(&tmp_path, path);
        }
    }

    let meta = std::fs::metadata(path).ok()?;
    let size = meta.len() as i64;
    let mtime = meta.modified()
        .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64)
        .unwrap_or(0);
    let filename = Path::new(path).file_name()?.to_str()?.to_string();

    Some((path.to_string(), filename, size, mtime))
}

pub fn run_ffprobe(path: &str, processed_tags: &[String]) -> (Option<String>, Option<String>, Option<String>, Option<String>) {
    let output = Command::new("ffprobe")
        .args(&[
            "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            path,
        ])
        .output();

    let output = match output {
        Ok(out) => out,
        Err(_) => return (None, None, None, None),
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value = match serde_json::from_str(&stdout) {
        Ok(j) => j,
        Err(_) => return (None, None, None, None),
    };

    let mut video_codec = None;
    let mut resolution = None;
    let mut audio_channels = None;
    let mut tag = None;

    if let Some(format_tags) = json.get("format").and_then(|f| f.get("tags")).and_then(|t| t.as_object()) {
        for (k, v) in format_tags {
            if k.eq_ignore_ascii_case("comment") {
                let val_str = v.as_str().unwrap_or("");
                if !val_str.is_empty() {
                    tag = Some(val_str.to_string());
                }
            }
        }
    }

    if tag.is_none() {
        if let Some(streams) = json.get("streams").and_then(|s| s.as_array()) {
            for stream in streams {
                let st_tags = stream.get("tags");
                if let Some(tags_obj) = st_tags.and_then(|t| t.as_object()) {
                    for (k, v) in tags_obj {
                        if k.eq_ignore_ascii_case("comment") {
                            let val_str = v.as_str().unwrap_or("");
                            if !val_str.is_empty() {
                                tag = Some(val_str.to_string());
                                break;
                            }
                        }
                    }
                }
                if tag.is_some() { break; }
            }
        }
    }

    if let Some(streams) = json.get("streams").and_then(|s| s.as_array()) {
        for stream in streams {
            let codec_type = stream.get("codec_type").and_then(|c| c.as_str()).unwrap_or("");
            let is_attached_pic = stream.get("disposition")
                .and_then(|d| d.get("attached_pic"))
                .and_then(|ap| ap.as_i64())
                .unwrap_or(0) == 1;

            let st_tags = stream.get("tags");

            if tag.is_none() {
                if let Some(tags_obj) = st_tags.and_then(|t| t.as_object()) {
                    for (k, v) in tags_obj {
                        if k.eq_ignore_ascii_case("comment") {
                            let val_str = v.as_str().unwrap_or("");
                            if processed_tags.iter().any(|pt| val_str.contains(pt)) {
                                tag = Some(val_str.to_string());
                            }
                        }
                    }
                }
            }

            if codec_type == "video" && !is_attached_pic && video_codec.is_none() {
                video_codec = stream.get("codec_name").and_then(|c| c.as_str()).map(|s| s.to_string());
                if let (Some(w), Some(h)) = (
                    stream.get("width").and_then(|w| w.as_i64()),
                    stream.get("height").and_then(|h| h.as_i64()),
                ) {
                    resolution = Some(format!("{}x{}", w, h));
                }
            } else if codec_type == "audio" && audio_channels.is_none() {
                audio_channels = stream.get("channels").and_then(|c| c.as_i64()).map(|ch| ch.to_string());
            }
        }
    }

    (video_codec, resolution, audio_channels, tag)
}

pub fn reevaluate_filters_from_db(
    db_path: &str,
    lock_file_path: &str,
    config: &Config,
    queue: &std::sync::Arc<std::sync::Mutex<Vec<crate::dispatch::QueueItem>>>,
    condvar: &std::sync::Arc<std::sync::Condvar>,
) {
    let lock_file = match std::fs::OpenOptions::new().read(true).write(true).create(true).open(lock_file_path) {
        Ok(f) => f,
        Err(_) => return,
    };
    if lock_file.lock_exclusive().is_err() {
        return;
    }

    let mut conn = match open_database(db_path) {
        Ok(c) => c,
        Err(_) => {
            let _ = lock_file.unlock();
            return;
        }
    };

    println!("🔄 [CONFIG RE-EVAL] Re-evaluating database records against updated filter rules (Zero disk/ffprobe scan)...");

    let updates_to_apply: Vec<(String, String, i64)> = {
        let mut stmt = match conn.prepare("SELECT path, size, mtime, video_codec, resolution, audio_channels, tag, status FROM media_files") {
            Ok(s) => s,
            Err(_) => {
                let _ = lock_file.unlock();
                return;
            }
        };

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
        });

        let mut collected = Vec::new();
        if let Ok(iter) = rows {
            for row_result in iter {
                if let Ok((path, size, mtime, v_codec, res, audio, tag, status)) = row_result {
                    if status == "completed" || status == "processing" || status.starts_with("dispatched_server_") {
                        continue;
                    }
                    if path.ends_with(".srt") {
                        continue;
                    }

                    let filename_lower = Path::new(&path).file_name().unwrap_or_default().to_string_lossy().to_lowercase();
                    let fc = get_applicable_filter_config(&path, config);
                    let matches_skip = fc.skip_terms.iter().any(|term| filename_lower.contains(&term.to_lowercase())) ||
                                       config.skip_terms.iter().any(|term| filename_lower.contains(&term.to_lowercase()));

                    let (passes, _) = file_passes_filters(
                        &path, size, mtime, v_codec.as_deref(), res.as_deref(), audio.as_deref(), tag.as_deref(), config
                    );

                    let new_status = if matches_skip {
                        "skip_term".to_string()
                    } else if passes {
                        "queued".to_string()
                    } else {
                        "filtered_out".to_string()
                    };

                    if status != new_status {
                        collected.push((path, new_status, size));
                    }
                }
            }
        }
        collected
    };

    if !updates_to_apply.is_empty() {
        if let Ok(tx) = conn.transaction() {
            let mut newly_queued = 0;
            for (path, new_status, size) in updates_to_apply {
                let _ = tx.execute(
                    "UPDATE media_files SET status = ?1, updated_at = ?2 WHERE path = ?3",
                    params![new_status, chrono::Utc::now().timestamp(), path]
                );

                if new_status == "queued" {
                    let mut q = queue.lock().unwrap();
                    q.push(crate::dispatch::QueueItem {
                        tier: 2,
                        size,
                        path,
                    });
                    q.sort();
                    drop(q);
                    newly_queued += 1;
                }
            }
            let _ = tx.commit();

            if newly_queued > 0 {
                println!("✨ [CONFIG RE-EVAL] Re-evaluation complete. Queued {} newly matching items.", newly_queued);
                condvar.notify_all();
            } else {
                println!("✨ [CONFIG RE-EVAL] Re-evaluation complete. Status transitions updated successfully.");
            }
        }
    } else {
        println!("✨ [CONFIG RE-EVAL] Re-evaluation complete. No status transitions triggered.");
    }

    let _ = lock_file.unlock();
}

pub fn sweep_and_reclean_all_srts(db_path: &str, lock_file_path: &str, config: &Config) {
    let lock_file = match std::fs::OpenOptions::new().read(true).write(true).create(true).open(lock_file_path) {
        Ok(f) => f,
        Err(_) => return,
    };
    if lock_file.lock_exclusive().is_err() {
        return;
    }

    let conn = match open_database(db_path) {
        Ok(c) => c,
        Err(_) => {
            let _ = lock_file.unlock();
            return;
        }
    };

    let clean_terms: Vec<String> = config.srt_clean_terms.iter().map(|t| t.to_lowercase()).collect();

    let needs_reclean = should_resweep_srts(&conn, &clean_terms).unwrap_or(true);

    if needs_reclean {
        println!("🧹 [SRT SWEEP] Clearing old SRT database records and re-cleaning all subtitles on disk...");
        
        let _ = conn.execute("DELETE FROM media_files WHERE path LIKE '%.srt' OR path LIKE '%.SRT'", []);
        let _ = lock_file.unlock();

        for sp in &config.global_source_paths {
            let path_obj = Path::new(sp);
            if !path_obj.exists() { continue; }

            for entry in walkdir::WalkDir::new(path_obj).into_iter().filter_map(|e| e.ok()) {
                if entry.file_type().is_file() {
                    let p = entry.path();
                    let p_str = p.display().to_string();
                    if p_str.to_lowercase().ends_with(".srt") {
                        if let Some((path, filename, size, mtime)) = process_and_clean_srt_disk(&p_str, &clean_terms) {
                            if let Ok(c_conn) = open_database(db_path) {
                                let _ = c_conn.execute(
                                    "INSERT INTO media_files (path, filename, size, mtime, tag, status, updated_at) 
                                     VALUES (?1, ?2, ?3, ?4, 'SRT_Cleaned', 'srt_cleaned', ?5)
                                     ON CONFLICT(path) DO UPDATE SET size = ?3, mtime = ?4, status = 'srt_cleaned', updated_at = ?5",
                                    params![path, filename, size, mtime, chrono::Utc::now().timestamp()],
                                );
                            }
                        }
                    }
                }
            }
        }

        if let Ok(c_conn) = open_database(db_path) {
            let _ = update_swept_terms_record(&c_conn, &clean_terms);
        }

        println!("✨ [SRT SWEEP] Subtitle re-clean sweep complete!");
    } else {
        let _ = lock_file.unlock();
    }
}

pub fn should_resweep_srts(conn: &Connection, current_terms: &[String]) -> Result<bool, rusqlite::Error> {
    let stored_json: Option<String> = conn.query_row(
        "SELECT value FROM app_meta WHERE key = 'last_swept_srt_terms'",
        [],
        |row| row.get(0),
    ).ok();

    let current_set: HashSet<&String> = current_terms.iter().collect();

    match stored_json {
        Some(json_str) => {
            let swept_terms: Vec<String> = serde_json::from_str(&json_str).unwrap_or_default();
            let swept_set: HashSet<&String> = swept_terms.iter().collect();

            let has_new_terms = current_set.iter().any(|term| !swept_set.contains(term));

            if has_new_terms {
                println!("🧹 [SRT SWEEP] New SRT clean term detected! Triggering sweep...");
                Ok(true)
            } else {
                println!("✨ [SRT SWEEP] No new SRT terms added. Skipping full library re-sweep.");
                Ok(false)
            }
        }
        None => {
            println!("🧹 [SRT SWEEP] No previous sweep record found. Running initial sweep...");
            Ok(true)
        }
    }
}

pub fn update_swept_terms_record(conn: &Connection, current_terms: &[String]) -> Result<(), rusqlite::Error> {
    let json_str = serde_json::to_string(current_terms).unwrap_or_else(|_| "[]".to_string());
    conn.execute(
        "INSERT OR REPLACE INTO app_meta (key, value) VALUES ('last_swept_srt_terms', ?1)",
        [json_str],
    )?;
    Ok(())
}