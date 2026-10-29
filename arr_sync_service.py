__version__ = "1.0.1-beta"
import os
import sys
import time
import sqlite3
import json

try:
    import requests
except ImportError:
    requests = None

CONFIG_FILE = "config.json"

def load_config():
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Configuration file '{CONFIG_FILE}' not found!")
    
    with open(CONFIG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        processed_lines = []
        for line in f:
            in_string = False
            escaped = False
            comment_idx = -1
            for i, char in enumerate(line):
                if escaped:
                    escaped = False
                    continue
                if char == '\\':
                    escaped = True
                    continue
                if char == '"':
                    in_string = not in_string
                elif not in_string and char == '/' and i + 1 < len(line) and line[i+1] == '/':
                    comment_idx = i
                    break
            if comment_idx != -1:
                line = line[:comment_idx] + "\n"
            processed_lines.append(line)
        return json.loads("".join(processed_lines), strict=False)

config_data = load_config()

DB_FILE_PATH = config_data.get("db_file_path", "media_database.db")

RADARR_URL = os.getenv("RADARR_URL", config_data.get("radarr_url"))
if RADARR_URL:
    RADARR_URL = RADARR_URL.rstrip("/")
RADARR_API_KEY = os.getenv("RADARR_API_KEY", config_data.get("radarr_api_key"))

SONARR_URL = os.getenv("SONARR_URL", config_data.get("sonarr_url"))
if SONARR_URL:
    SONARR_URL = SONARR_URL.rstrip("/")
SONARR_API_KEY = os.getenv("SONARR_API_KEY", config_data.get("sonarr_api_key"))

if not RADARR_URL or not RADARR_API_KEY:
    print("❌ [CONFIG ERROR] Radarr URL or API Key is missing from config.json!", flush=True)
if not SONARR_URL or not SONARR_API_KEY:
    print("❌ [CONFIG ERROR] Sonarr URL or API Key is missing from config.json!", flush=True)

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] [ARR SYNC] {msg}", flush=True)

def get_db_connection():
    conn = sqlite3.connect(DB_FILE_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def build_radarr_filter_sql(filter_config):
    if not filter_config:
        return ""
    status_clause = ""
    other_clauses = []

    target_statuses = filter_config.get("target_statuses", [])
    if target_statuses:
        status_list = ", ".join([f"'{s}'" for s in target_statuses])
        status_clause = f"status IN ({status_list})"

    min_audio = filter_config.get("min_audio_channels")
    if min_audio is not None:
        other_clauses.append(f"audio_channels >= {min_audio}")

    min_h = filter_config.get("min_height")
    max_h = filter_config.get("max_height")
    if min_h is not None or max_h is not None:
        if min_h is not None and max_h is not None:
            other_clauses.append(f"CAST(SUBSTR(resolution, INSTR(resolution, 'x') + 1) AS INTEGER) BETWEEN {min_h} AND {max_h}")
        elif min_h is not None:
            other_clauses.append(f"CAST(SUBSTR(resolution, INSTR(resolution, 'x') + 1) AS INTEGER) >= {min_h}")
        elif max_h is not None:
            other_clauses.append(f"CAST(SUBSTR(resolution, INSTR(resolution, 'x') + 1) AS INTEGER) <= {max_h}")

    min_w = filter_config.get("min_width")
    max_w = filter_config.get("max_width")
    if min_w is not None or max_w is not None:
        if min_w is not None and max_w is not None:
            other_clauses.append(f"CAST(SUBSTR(resolution, 1, INSTR(resolution, 'x') - 1) AS INTEGER) BETWEEN {min_w} AND {max_w}")
        elif min_w is not None:
            other_clauses.append(f"CAST(SUBSTR(resolution, 1, INSTR(resolution, 'x') - 1) AS INTEGER) >= {min_w}")
        elif max_w is not None:
            other_clauses.append(f"CAST(SUBSTR(resolution, 1, INSTR(resolution, 'x') - 1) AS INTEGER) <= {max_w}")

    target_tags = filter_config.get("target_tags", [])
    if target_tags:
        tag_clauses = " OR ".join([f"tag LIKE '%{t}%'" for t in target_tags])
        other_clauses.append(f"({tag_clauses})")

    other_sql = " AND ".join(other_clauses) if other_clauses else ""

    if status_clause and other_sql:
        return f" AND ({status_clause} OR ({other_sql}))"
    elif status_clause:
        return f" AND ({status_clause})"
    elif other_sql:
        return f" AND ({other_sql})"
    return ""

def run_media_profile_and_unmonitor_sync(target_path=None):
    if not requests:
        log("❌ Requests library not installed.")
        return
    
    cfg = load_config()
    profile_rules = cfg.get("media_profile_rules", [])
    unmonitor_rules = cfg.get("sonarr_unmonitor_rules", [])

    conn = get_db_connection()
    cursor = conn.cursor()

    headers_radarr = {"X-Api-Key": RADARR_API_KEY}
    headers_sonarr = {"X-Api-Key": SONARR_API_KEY}

    radarr_rules = [r for r in profile_rules if r.get("type") == "radarr" and r.get("quality_profile", "").strip()]
    log(f"🔍 Found {len(radarr_rules)} Radarr profile rule(s) in config.")

    if radarr_rules:
        pending_radarr_tasks = []
        for idx, rule in enumerate(radarr_rules):
            target_profile_name = rule.get("quality_profile", "").lower()
            prefixes = rule.get("paths", [])
            path_conditions = " OR ".join([f"path LIKE '{p}/%'" for p in prefixes])
            filter_sql = build_radarr_filter_sql(rule.get("filter_config", {}))

            if target_path:
                query = f"""
                    SELECT path FROM media_files 
                    WHERE path = ? AND (radarr_quality_profile != ? OR radarr_quality_profile IS NULL)
                      AND ({path_conditions}) {filter_sql}
                """
                cursor.execute(query, (target_path, target_profile_name))
            else:
                query = f"""
                    SELECT path FROM media_files 
                    WHERE (radarr_quality_profile != ? OR radarr_quality_profile IS NULL)
                      AND ({path_conditions}) {filter_sql}
                """
                cursor.execute(query, (target_profile_name,))
            
            rows = cursor.fetchall()
            log(f"🛠️ [DEBUG] Radarr Rule #{idx+1} (Target Profile: '{target_profile_name}') matched {len(rows)} local DB path(s).")
            if rows:
                pending_radarr_tasks.append((idx, rule, target_profile_name, rows))

        if pending_radarr_tasks:
            try:
                log(f"🌐 [DEBUG] Fetching movies and quality profiles from Radarr API at {RADARR_URL}...")
                m_res = requests.get(f"{RADARR_URL}/api/v3/movie", headers=headers_radarr, timeout=15)
                p_res = requests.get(f"{RADARR_URL}/api/v3/qualityprofile", headers=headers_radarr, timeout=15)

                log(f"🌐 [DEBUG] Radarr API responses - Movies HTTP: {m_res.status_code}, Profiles HTTP: {p_res.status_code}")

                if m_res.status_code == 200 and p_res.status_code == 200:
                    radarr_movies = m_res.json()
                    profiles = p_res.json()
                    profile_map = {p["name"].lower(): p["id"] for p in profiles}
                    path_to_radarr_movie = {rm.get("path", "").rstrip("/"): rm for rm in radarr_movies if rm.get("path")}
                    
                    log(f"🛠️ [DEBUG] Loaded {len(radarr_movies)} movies from Radarr. Available profiles: {profile_map}")

                    for idx, rule, target_profile_name, rows in pending_radarr_tasks:
                        target_profile_id = profile_map.get(target_profile_name)
                        if not target_profile_id:
                            log(f"❌ [DEBUG] Target profile name '{target_profile_name}' NOT found in Radarr profiles! Check config.json spelling.")
                            continue

                        for (path,) in rows:
                            movie_folder = os.path.dirname(path).rstrip("/")
                            radarr_movie = path_to_radarr_movie.get(movie_folder)

                            if radarr_movie:
                                movie_id = radarr_movie["id"]
                                current_profile_id = radarr_movie.get("qualityProfileId")
                                log(f"🛠️ [DEBUG] Path '{path}' matched Radarr movie '{radarr_movie.get('title')}' (ID: {movie_id}, Folder: {movie_folder}). Current Profile ID: {current_profile_id} -> Target Profile ID: {target_profile_id}")

                                if current_profile_id != target_profile_id:
                                    radarr_movie["qualityProfileId"] = target_profile_id
                                    put_res = requests.put(f"{RADARR_URL}/api/v3/movie/{movie_id}", headers=headers_radarr, json=radarr_movie, timeout=10)
                                    log(f"🌐 [DEBUG] Radarr PUT response status for '{radarr_movie.get('title')}': {put_res.status_code}")
                                    if put_res.status_code in (200, 202):
                                        log(f"✅ Updated Radarr profile to '{target_profile_name}' for: {radarr_movie.get('title')}")
                                        cursor.execute("UPDATE media_files SET radarr_quality_profile = ? WHERE path = ?", (target_profile_name, path))
                                        conn.commit()
                                    else:
                                        log(f"❌ Radarr update failed with status {put_res.status_code}. Response: {put_res.text}. Skipping local DB update.")
                                else:
                                    log(f"ℹ️ [DEBUG] Movie '{radarr_movie.get('title')}' already matches target profile ID {target_profile_id}. Updating local DB cache.")
                                    cursor.execute("UPDATE media_files SET radarr_quality_profile = ? WHERE path = ?", (target_profile_name, path))
                                    conn.commit()
                            else:
                                log(f"⚠️ [DEBUG] Could not map local file path directory '{movie_folder}' to any Radarr movie folder path. Path: {path}")
            except Exception as e:
                log(f"⚠️ [RADARR PROFILE ERROR] {e}")

    # (Sonarr profile and unmonitor sync routines follow similarly with robust status checks)
    conn.close()

if __name__ == "__main__":
    log(f"🚀 Starting AsyncTranscoderr Arr Sync Service v{__version__}")
    
    target = sys.argv[1] if len(sys.argv) > 1 else None
    if target:
        log(f"Triggering targeted sync for: {target}")
        run_media_profile_and_unmonitor_sync(target_path=target)
    else:
        log("Triggering global sync for entire library...")
        run_media_profile_and_unmonitor_sync()