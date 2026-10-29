__version__ = "1.0.1-beta"

import os
import time
import shutil
import json
import subprocess
import threading
import logging
from logging.handlers import RotatingFileHandler
import sys
import signal

# Thread-safe lock for backoff tracking dictionary
backoff_lock = threading.Lock()
file_backoffs = {}

# Global tracker for active FFmpeg subprocess to enable clean termination on reload/shutdown
current_ffmpeg_process = None

def can_try_file(filepath):
    with backoff_lock:
        if filepath not in file_backoffs:
            return True
        return time.time() >= file_backoffs[filepath]["next_try"]

def record_failure(filepath, error_msg, global_cfg):
    delays = global_cfg.get("backoff_delays", [10, 30, 120, 300, 900])
    with backoff_lock:
        if filepath not in file_backoffs:
            file_backoffs[filepath] = {"attempts": 1, "next_try": time.time() + delays[0]}
            wait_time = delays[0]
        else:
            attempts = file_backoffs[filepath]["attempts"] + 1
            delay_idx = min(attempts - 1, len(delays) - 1)
            wait_time = delays[delay_idx]
            file_backoffs[filepath] = {"attempts": attempts, "next_try": time.time() + wait_time}
        
        attempts_count = file_backoffs[filepath]["attempts"]
    
    logging.warning(f"⚠️ [NETWORK BACKOFF] File '{os.path.basename(filepath)}' failed attempt #{attempts_count} ({error_msg}). Backing off for {wait_time}s.")

def clear_failure(filepath):
    with backoff_lock:
        if filepath in file_backoffs:
            del file_backoffs[filepath]

def cleanup_old_backoffs():
    while True:
        time.sleep(3600)
        now = time.time()
        with backoff_lock:
            expired = [path for path, data in file_backoffs.items() if now - data["next_try"] > 86400]
            for path in expired:
                del file_backoffs[path]

def load_config():
    if not os.path.exists("config_node_sa.json"):
        raise FileNotFoundError("config_node_sa.json not found in working directory.")
    with open("config_node_sa.json", "r") as f:
        config = json.load(f)
    
    # 1. Look for Docker environment variable first
    # 2. Look for "current_node_id" in the JSON file next
    node_id = os.getenv("NODE_ID") or config.get("current_node_id")
    
    server_cfg = None
    if node_id:
        server_cfg = next((s for s in config["servers"] if s["id"] == node_id), None)
    
    # 3. SAFETY NET: If neither was provided or found, gracefully use the FIRST server!
    if not server_cfg and config.get("servers"):
        server_cfg = config["servers"][0]
        logging.info(f"ℹ️ [CONFIG] No node ID matched. Defaulting to first server block: '{server_cfg.get('id')}'")
    
    if not server_cfg:
        raise ValueError("No server configurations found in config_node_sa.json servers list.")
    
    return server_cfg, config

def setup_logging(server_cfg):
    log_handlers = [logging.StreamHandler(sys.stdout)]
    log_file = server_cfg.get("log_file_path")
    if log_file:
        max_bytes = server_cfg.get("log_max_bytes", 10 * 1024 * 1024)
        backup_count = server_cfg.get("log_backup_count", 3)
        rotating_handler = RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
        )
        log_handlers.append(rotating_handler)

    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
        handlers=log_handlers
    )

def is_file_stable(filepath, wait_time=5):
    try:
        size1 = os.path.getsize(filepath)
        time.sleep(wait_time)
        size2 = os.path.getsize(filepath)
        return size1 == size2 and size1 > 0
    except Exception:
        return False

def perform_startup_cleanup(server_cfg):
    process_dir = server_cfg.get("path_process")
    if not process_dir or not os.path.exists(process_dir):
        return

    logging.info(f"🧹 [STARTUP CLEANUP] Checking processing directory: {process_dir}")
    try:
        for filename in os.listdir(process_dir):
            filepath = os.path.join(process_dir, filename)
            if os.path.isfile(filepath):
                if filename.lower().endswith('.tmp') or '.tmp' in filename.lower():
                    logging.info(f"🛡️ [PROTECTED] Leaving .tmp file intact: {filename}")
                    continue

                if '_processing' in filename.lower() or 'raw_tmp' in filename.lower() or '_converted.' in filename.lower():
                    os.remove(filepath)
                    logging.info(f"🗑️ [CLEANUP] Removed stale file: {filename}")
    except Exception as e:
        logging.error(f"Error during startup cleanup: {e}")

def check_if_already_tagged(filepath, server_cfg, global_cfg):
    ffprobe_bin = server_cfg.get("ffprobe_bin", "ffprobe")
    retag_id = server_cfg.get("retag_identifier", "")
    tag_ids = server_cfg.get("tag_identifier", [])
    timeout_val = global_cfg.get("ffprobe_timeout_seconds", 10)
    
    if isinstance(tag_ids, str):
        tag_ids = [tag_ids]

    check_mode = server_cfg.get("check_already_tagged")

    if not check_mode or not (retag_id or tag_ids):
        return False

    try:
        cmd = [
            ffprobe_bin, "-v", "quiet", "-print_format", "json",
            "-show_entries", "format_tags=comment:stream_tags=comment",
            str(filepath)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_val)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            format_tags = data.get("format", {}).get("tags", {})
            for k, v in format_tags.items():
                if k.lower() == "comment":
                    val_str = str(v)
                    if retag_id and retag_id in val_str:
                        return True
                    if any(t in val_str for t in tag_ids):
                        return True

            for stream in data.get("streams", []):
                stream_tags = stream.get("tags", {})
                for k, v in stream_tags.items():
                    if k.lower() == "comment":
                        val_str = str(v)
                        if retag_id and retag_id in val_str:
                            return True
                        if any(t in val_str for t in tag_ids):
                            return True
    except Exception:
        pass
    return False

def is_english_language(lang_str):
    if not lang_str:
        return False
    clean_lang = lang_str.lower().strip()
    base_lang = clean_lang.split("-")[0].split("_")[0]
    return base_lang in {"eng", "en", "english"}

def get_ffmpeg_audio_subtitle_args(file_path, server_cfg, global_cfg):
    ffprobe_bin = server_cfg.get("ffprobe_bin", "ffprobe")
    timeout_val = global_cfg.get("ffprobe_timeout_seconds", 10)
    cmd = [ffprobe_bin, "-v", "error", "-show_streams", "-of", "json", file_path]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_val)
        data = json.loads(res.stdout)
        streams = data.get("streams", [])
    except Exception as e:
        logging.error(f"⚠️ [FFPROBE PARSE ERROR] {e}")
        streams = []

    audio_streams = [s for s in streams if s.get("codec_type") == "audio"]
    sub_streams = [s for s in streams if s.get("codec_type") == "subtitle"]

    maps = ["-map", "0:v:0"]

    audio_mode = server_cfg.get("audio_tracks", "keep_english_fallback_all")
    sub_mode = server_cfg.get("subtitle_tracks", "keep_english_and_forced")

    if audio_mode == "keep_all":
        maps.extend(["-map", "0:a?"])
    else:
        eng_audio = []
        for s in audio_streams:
            idx = s.get("index")
            tags = s.get("tags") or {}
            lang = str(tags.get("language", ""))
            if is_english_language(lang):
                eng_audio.append(idx)

        if eng_audio:
            for idx in eng_audio:
                maps.extend(["-map", f"0:{idx}"])
        else:
            logging.info("ℹ️ [AUDIO] No explicit English audio tag found. Falling back to all audio streams.")
            maps.extend(["-map", "0:a?"])

    if sub_mode in ["drop", "none"]:
        logging.info("ℹ️ [SUBTITLES] Subtitle stripping enabled in config. Disabling subtitles.")
    elif sub_mode == "keep_all":
        maps.extend(["-map", "0:s?"])
    else:
        eng_subs = []
        for s in sub_streams:
            idx = s.get("index")
            tags = s.get("tags") or {}
            lang = str(tags.get("language", ""))
            disposition = s.get("disposition", {})
            is_forced = disposition.get("forced", 0) == 1

            if is_english_language(lang) or (is_forced and is_english_language(lang)):
                eng_subs.append(idx)

        if eng_subs:
            for idx in eng_subs:
                maps.extend(["-map", f"0:{idx}"])
        else:
            logging.info("ℹ️ [SUBTITLES] No English/forced subtitle tag found. Falling back to all subtitle streams.")
            maps.extend(["-map", "0:s?"])

    maps.extend(["-map", "0:t?"])
    return maps

def process_srt(f, global_cfg):
    logging.info(f"🧹 [SRT] Cleaning: {os.path.basename(f)}")
    srt_skip_terms = global_cfg.get("srt_skip_terms", [])
    tmp = f + ".tmp"
    try:
        with open(f, "r", encoding="utf-8", errors="ignore") as src, open(tmp, "w", encoding="utf-8") as dst:
            for line in src:
                if not any(p.lower() in line.lower() for p in srt_skip_terms):
                    dst.write(line)
        shutil.move(tmp, f)
    except Exception as e:
        logging.error(f"⚠️ [SRT ERROR] {e}")

def safe_move(src_path, target_dir, filename, global_cfg):
    if not can_try_file(src_path):
        return False

    os.makedirs(target_dir, exist_ok=True)
    dest = os.path.join(target_dir, filename)
    dest_tmp = dest + ".tmp"

    try:
        for target in [dest, dest_tmp]:
            if os.path.exists(target):
                if os.path.isdir(target):
                    shutil.rmtree(target)
                else:
                    os.remove(target)

        shutil.move(src_path, dest_tmp)
        os.rename(dest_tmp, dest)
        clear_failure(src_path)
        return True
    except Exception as e:
        record_failure(src_path, str(e), global_cfg)
        return False

def watcher_worker(server_cfg, global_cfg):
    local_mode = server_cfg.get("local_mode", False)
    monitor_dir = server_cfg["path_monitor"]
    process_dir = server_cfg["path_process"]
    stage_grab = server_cfg.get("path_stage_grab")
    final_completed = server_cfg["path_final_completed"]
    skip_terms = [t.lower() for t in global_cfg.get("skip_terms", [])]
    stability_wait = global_cfg.get("file_stability_wait_seconds", 5)
    poll_interval = global_cfg.get("poll_interval_seconds", 5)

    os.makedirs(process_dir, exist_ok=True)
    if stage_grab:
        os.makedirs(stage_grab, exist_ok=True)

    logging.info(f"👀 [WATCHER] Active monitoring: {monitor_dir}")

    while True:
        try:
            if not os.path.exists(monitor_dir):
                time.sleep(poll_interval)
                continue

            for filename in os.listdir(monitor_dir):
                src_path = os.path.join(monitor_dir, filename)
                if os.path.isdir(src_path) or not can_try_file(src_path):
                    continue

                if any(term in filename.lower() for term in skip_terms):
                    logging.info(f"⏭️ [SKIP TERM] Bypassing encode: {filename}")
                    dest_dir = final_completed if local_mode else process_dir
                    safe_move(src_path, dest_dir, filename, global_cfg)
                    continue

                if is_file_stable(src_path, wait_time=stability_wait):
                    if local_mode:
                        dest_path = os.path.join(process_dir, filename)
                        logging.info(f"📥 [LOCAL GRAB] Moving to process: {filename}")
                        safe_move(src_path, dest_path, filename, global_cfg)
                    else:
                        staged_path = os.path.join(stage_grab, filename)
                        staged_tmp = staged_path + ".tmp"
                        logging.info(f"📥 [NON-LOCAL GRAB] Staging atomically: {filename}")
                        try:
                            shutil.copy2(src_path, staged_tmp)
                            if os.path.exists(staged_path):
                                os.remove(staged_path)
                            os.rename(staged_tmp, staged_path)

                            logging.info(f"🚚 [NON-LOCAL MOVE] Moving to process: {filename}")
                            if safe_move(staged_path, process_dir, filename, global_cfg):
                                os.remove(src_path)
                                clear_failure(src_path)
                        except Exception as e:
                            if os.path.exists(staged_tmp):
                                try:
                                    os.remove(staged_tmp)
                                except:
                                    pass
                            record_failure(src_path, str(e), global_cfg)

        except Exception as e:
            logging.error(f"Error in watcher thread: {e}")

        time.sleep(poll_interval)

def encoder_worker(server_cfg, global_cfg):
    global current_ffmpeg_process
    local_mode = server_cfg.get("local_mode", False)
    process_dir = server_cfg["path_process"]
    stage_completed = server_cfg.get("path_stage_completed")
    stage_failed = server_cfg.get("path_stage_failed")
    final_completed = server_cfg["path_final_completed"]
    final_failed = server_cfg["path_final_failed"]
    ffmpeg_bin = server_cfg["ffmpeg_bin"]
    poll_interval = global_cfg.get("poll_interval_seconds", 5)
    
    tag_ids = server_cfg.get("tag_identifier", "AsyncTranscoderr")
    if isinstance(tag_ids, list):
        tag_id = tag_ids[0] if tag_ids else "AsyncTranscoderr"
    else:
        tag_id = tag_ids

    check_tagged_mode = server_cfg.get("check_already_tagged")

    high_terms = [t.lower() for t in global_cfg.get("high_priority_terms", [])]
    medium_terms = [t.lower() for t in global_cfg.get("medium_priority_terms", [])]

    os.makedirs(process_dir, exist_ok=True)
    if stage_completed:
        os.makedirs(stage_completed, exist_ok=True)
    if stage_failed:
        os.makedirs(stage_failed, exist_ok=True)

    logging.info(f"🎬 [ENCODER] Active on processing folder: {process_dir}")

    while True:
        try:
            for f_name in os.listdir(process_dir):
                if f_name.lower().endswith(".srt"):
                    process_srt(os.path.join(process_dir, f_name), global_cfg)

            raw_files = [
                f for f in os.listdir(process_dir) 
                if f.lower().endswith(('.mp4', '.mkv')) 
                and '_converted.' not in f.lower() 
                and '.tmp' not in f.lower()
                and can_try_file(os.path.join(process_dir, f))
            ]

            def file_sort_key(filename):
                file_path = os.path.join(process_dir, filename)
                try:
                    file_size = os.path.getsize(file_path)
                except Exception:
                    file_size = 0
                
                lname = filename.lower()
                if any(t in lname for t in high_terms):
                    tier = 0
                elif any(t in lname for t in medium_terms):
                    tier = 1
                else:
                    tier = 2
                return (tier, -file_size)

            files = sorted(raw_files, key=file_sort_key)
            
            for filename in files:
                src_file = os.path.join(process_dir, filename)
                
                if check_tagged_mode and check_if_already_tagged(src_file, server_cfg, global_cfg):
                    if check_tagged_mode == "skip":
                        logging.info(f"⏭️ [ALREADY TAGGED] Skipping file: {filename}")
                        target_dir = final_completed if local_mode else stage_completed
                        safe_move(src_file, target_dir, filename, global_cfg)
                        continue
                    elif check_tagged_mode == "strip_and_retag":
                        logging.info(f"🏷️ [ALREADY TAGGED] Strip & retag mode: re-processing tracks for {filename}")

                base, ext = os.path.splitext(filename)
                out_ext = ".mp4" if ext.lower() == ".mp4" else ".mkv"
                out_file = os.path.join(process_dir, f"{base}_converted{out_ext}")
                sub_codec = "mov_text" if out_ext == ".mp4" else "copy"

                audio_sub_args = get_ffmpeg_audio_subtitle_args(src_file, server_cfg, global_cfg)

                encoding_args = ["-c:v", "libx265", "-pix_fmt", "yuv420p", "-tag:v", "hvc1"]
                crf_val = server_cfg.get("crf")
                if crf_val is not None:
                    encoding_args.extend(["-crf", str(crf_val)])
                
                preset_val = server_cfg.get("preset")
                if preset_val:
                    encoding_args.extend(["-preset", str(preset_val)])

                extra_args = server_cfg.get("extra_ffmpeg_args", [])

                logging.info(f"🎬 [ENCODING START] -> {filename}")

                cmd = (
                    [ffmpeg_bin, "-i", src_file]
                    + audio_sub_args
                    + ["-map_metadata", "0"]
                    + encoding_args
                    + [
                        "-c:a", "copy", "-c:s", sub_codec,
                        "-metadata", f"comment={tag_id}",
                        "-metadata:s", f"comment={tag_id}",
                    ]
                    + extra_args
                    + ["-y", out_file]
                )

                current_ffmpeg_process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                current_ffmpeg_process.wait()
                retcode = current_ffmpeg_process.returncode
                current_ffmpeg_process = None

                if retcode == 0 and os.path.exists(out_file):
                    os.remove(src_file)
                    os.rename(out_file, src_file)

                    target_dir = final_completed if local_mode else stage_completed
                    safe_move(src_file, target_dir, filename, global_cfg)
                    logging.info(f"✅ [ENCODE SUCCESS] {filename}")
                else:
                    if os.path.exists(out_file):
                        os.remove(out_file)
                    target_dir = final_failed if local_mode else stage_failed
                    if os.path.exists(src_file):
                        safe_move(src_file, target_dir, filename, global_cfg)
                    logging.error(f"❌ [ENCODE FAILED] {filename}")

        except Exception as e:
            logging.error(f"Error in encoder thread: {e}")

        time.sleep(poll_interval)

def mover_worker(server_cfg, global_cfg):
    if server_cfg.get("local_mode", False):
        return

    stage_completed = server_cfg["path_stage_completed"]
    final_completed = server_cfg["path_final_completed"]
    stage_failed = server_cfg["path_stage_failed"]
    final_failed = server_cfg["path_final_failed"]
    poll_interval = global_cfg.get("poll_interval_seconds", 5)

    os.makedirs(stage_completed, exist_ok=True)
    os.makedirs(final_completed, exist_ok=True)
    os.makedirs(stage_failed, exist_ok=True)
    os.makedirs(final_failed, exist_ok=True)

    logging.info(f"🚚 [MOVER] Active for network staging finalization.")

    while True:
        try:
            for folder, target in [(stage_completed, final_completed), (stage_failed, final_failed)]:
                if not os.path.exists(folder):
                    continue
                for filename in os.listdir(folder):
                    src = os.path.join(folder, filename)
                    if os.path.isfile(src) and '.tmp' not in filename.lower() and can_try_file(src):
                        logging.info(f"🚚 [FINAL MOVE] Flushing -> {filename}")
                        safe_move(src, target, filename, global_cfg)

        except Exception as e:
            logging.error(f"Error in mover worker: {e}")

        time.sleep(poll_interval)

def config_watcher_worker(config_path="config_node_sa.json"):
    global current_ffmpeg_process
    if not os.path.exists(config_path):
        return
    
    last_mtime = os.path.getmtime(config_path)
    
    while True:
        time.sleep(3)
        try:
            if os.path.exists(config_path):
                current_mtime = os.path.getmtime(config_path)
                if current_mtime != last_mtime:
                    logging.info("⚙️ [CONFIG WATCHER] Changes detected in config_node_sa.json. Terminating active FFmpeg and restarting script...")
                    
                    if current_ffmpeg_process and current_ffmpeg_process.poll() is None:
                        try:
                            current_ffmpeg_process.terminate()
                            current_ffmpeg_process.wait(timeout=3)
                            logging.info("🛑 [CONFIG WATCHER] Active FFmpeg process terminated successfully.")
                        except Exception:
                            current_ffmpeg_process.kill()

                    time.sleep(1)
                    os.execv(sys.executable, [sys.executable] + sys.argv)
        except Exception as e:
            logging.error(f"Error in config watcher thread: {e}")

def handle_shutdown(signum, frame):
    global current_ffmpeg_process
    logging.info("🛑 [SHUTDOWN] Signal received. Terminating active FFmpeg and exiting...")
    if current_ffmpeg_process and current_ffmpeg_process.poll() is None:
        try:
            current_ffmpeg_process.terminate()
            current_ffmpeg_process.wait(timeout=3)
        except Exception:
            current_ffmpeg_process.kill()
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    server_cfg, global_cfg = load_config()
    setup_logging(server_cfg)
    
    logging.info(f"🚀 [NODE SERVICE] Starting up v{__version__} (Node ID: {server_cfg['id']}, Local Mode: {server_cfg.get('local_mode', False)})")

    perform_startup_cleanup(server_cfg)

    threads = [
        threading.Thread(target=watcher_worker, args=(server_cfg, global_cfg), daemon=True),
        threading.Thread(target=encoder_worker, args=(server_cfg, global_cfg), daemon=True),
        threading.Thread(target=config_watcher_worker, args=("config_node_sa.json",), daemon=True),
        threading.Thread(target=cleanup_old_backoffs, daemon=True)
    ]

    if not server_cfg.get("local_mode", False):
        threads.append(threading.Thread(target=mover_worker, args=(server_cfg, global_cfg), daemon=True))

    for t in threads:
        t.start()

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()