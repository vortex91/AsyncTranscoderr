mod common;
mod scan;
mod watch;
mod dispatch;

use common::Config;
use std::sync::{Arc, RwLock};
use std::sync::mpsc::channel;
use std::time::Duration;
use std::fs;
use chrono::Timelike;
use std::sync::atomic::{AtomicBool, Ordering};

static SHUTDOWN_FLAG: AtomicBool = AtomicBool::new(false);

fn main() {
    ctrlc::set_handler(move || {
        println!("\n🛑 [SHUTDOWN] Received system shutdown signal (SIGINT/SIGTERM). Shutting down gracefully...");
        SHUTDOWN_FLAG.store(true, Ordering::SeqCst);
        
        std::thread::sleep(std::time::Duration::from_secs(2));
        std::process::exit(0);
    }).expect("Error setting Ctrl-C handler");

    println!("🚀 [MEDIA MANAGER] Starting media_manager v{}", env!("CARGO_PKG_VERSION"));

    let args: Vec<String> = std::env::args().collect();
    let subcommand = args.get(1).map(|s| s.as_str()).unwrap_or("all");

    let config_path = "config.json";
    
    let initial_config = common::load_config(config_path).unwrap_or_else(|e| {
        eprintln!("❌ [CONFIG ERROR] {}", e);
        std::process::exit(1);
    });

    let db_path = initial_config.database_path.clone().unwrap_or_else(|| "media_database.db".to_string());
    let lock_path = format!("{}.lock", db_path);

    if let Ok(mut conn) = common::open_database(&db_path) {
        common::init_database(&mut conn);
    }
    common::sweep_and_reclean_all_srts(&db_path, &lock_path, &initial_config);

    let config = Arc::new(RwLock::new(initial_config));

    let dispatcher_handle = dispatch::run(&config);

    let config_clone = Arc::clone(&config);
    let disp_handle_for_reload = dispatcher_handle.clone();
    std::thread::spawn(move || {
        let mut last_mtime = fs::metadata(config_path).and_then(|m| m.modified()).ok();
        loop {
            std::thread::sleep(Duration::from_secs(3));
            if let Ok(meta) = fs::metadata(config_path) {
                if let Ok(mtime) = meta.modified() {
                    if Some(mtime) != last_mtime {
                        last_mtime = Some(mtime);
                        match common::load_config(config_path) {
                            Ok(new_config) => {
                                let mut w = config_clone.write().unwrap();
                                *w = new_config.clone();
                                drop(w);
                                println!("🔄 [CONFIG] Live-reloaded config.json successfully on the fly!");

                                let reload_db_path = new_config.database_path.clone().unwrap_or_else(|| "media_database.db".to_string());
                                let reload_lock_path = format!("{}.lock", reload_db_path);

                                common::reevaluate_filters_from_db(
                                    &reload_db_path, 
                                    &reload_lock_path, 
                                    &new_config, 
                                    &disp_handle_for_reload.queue, 
                                    &disp_handle_for_reload.condvar
                                );

                                common::sweep_and_reclean_all_srts(&reload_db_path, &reload_lock_path, &new_config);
                            }
                            Err(e) => {
                                eprintln!("❌ [CONFIG ERROR] {}. Keeping previous config.", e);
                            }
                        }
                    }
                }
            }
        }
    });

    let (watch_tx, watch_rx) = channel::<()>();

    let config_for_watch = Arc::clone(&config);
    let dispatcher_handle_for_watch = dispatcher_handle.clone();
    std::thread::spawn(move || {
        watch::run_with_signal(&config_for_watch, &dispatcher_handle_for_watch, watch_tx);
    });

    match subcommand {
        "scan" => {
            println!("🚀 [ROUTER] Launching Full Scan only...");
            scan::run(&config, &dispatcher_handle);
        }
        "watch" => {
            println!("👀 [ROUTER] Real-Time Watcher running in background. Entering monitoring loop...");
            loop {
                std::thread::sleep(Duration::from_secs(3600));
            }
        }
        "all" => {
            println!("👀 [ROUTER] Waiting for real-time file watcher to initialize on NFS mounts...");
            let _ = watch_rx.recv();
            
            println!("🚀👀 [ROUTER] Watcher active! Launching Initial Backlog Scan via Rayon...");
            scan::run(&config, &dispatcher_handle);
            println!("✨ [ROUTER] Initial backlog scan complete. Watcher is actively monitoring live events...");
            
            run_scheduler(config, dispatcher_handle);
        }
        other => {
            eprintln!("❌ Unknown subcommand: '{}'. Use 'all', 'scan', or 'watch'.", other);
            std::process::exit(1);
        }
    }
}

fn run_scheduler(config: Arc<RwLock<Config>>, dispatcher_handle: dispatch::DispatcherHandle) {
    println!("⏰ [SCHEDULER] Background scan scheduler initialized.");
    let config_clone = Arc::clone(&config);
    let disp_handle = dispatcher_handle.clone();

    std::thread::spawn(move || {
        let mut last_run_hour = chrono::Utc::now().hour();
        loop {
            std::thread::sleep(Duration::from_secs(60));

            let cfg = config_clone.read().unwrap();
            let interval_hours = cfg.scan_interval_hours;
            let time_of_day = cfg.scan_time_of_day.clone();
            drop(cfg);

            let now = chrono::Utc::now();
            let current_hour = now.hour();
            let current_minute = now.minute();
            let mut should_run = false;

            if let Some(hours) = interval_hours {
                if hours > 0 && current_hour != last_run_hour && (current_hour as u64) % hours == 0 && current_minute == 0 {
                    should_run = true;
                    last_run_hour = current_hour;
                }
            }

            if let Some(ref tod) = time_of_day {
                let parts: Vec<&str> = tod.split(':').collect();
                if parts.len() == 2 {
                    if let (Ok(h), Ok(m)) = (parts[0].parse::<u32>(), parts[1].parse::<u32>()) {
                        if current_hour == h && current_minute == m {
                            std::thread::sleep(Duration::from_secs(60));
                            should_run = true;
                        }
                    }
                }
            }

            if should_run {
                println!("⏰ [SCHEDULER] Scheduled scan triggered at {}...", now.format("%Y-%m-%d %H:%M:%S"));
                scan::run(&config_clone, &disp_handle);
            }
        }
    });

    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}