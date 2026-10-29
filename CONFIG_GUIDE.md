# AsyncTranscoderr Unified Configuration Guide (`CONFIG_GUIDE.md`)

This guide provides a comprehensive breakdown of every configuration parameter, multi-node worker setup, path-filtering rule, Arr integration option, and standalone node configuration available in **AsyncTranscoderr**.

---

## Architecture Overview

`AsyncTranscoderr` uses a decoupled orchestrator-worker architecture:
1. **Main Server (`config.json`)**: Manages global settings, metadata databases, library reconciliation, Radarr/Sonarr integration, and defines the `servers` array that queues and dispatches transcoding tasks.
2. **Worker Node (`config_node_sa.json`)**: Runs independently on local or remote hardware to process assigned tasks.

---

## 1. Core Server Configuration (`config.json`) Parameters

* `enable_gui` *(boolean)*: Toggles the built-in web management interface. Set to `false` to run as a headless background daemon, or `true` to enable web monitoring.
* `database_path` *(string)*: Absolute path to the SQLite metadata database (optimized in WAL mode).
* `scan_interval_hours` *(integer)*: Frequency of automated library reconciliation scans in hours (e.g., `6`).
* `scan_time_of_day` *(string)*: Specific daily time (`HH:MM` format) to trigger deep consistency checks (e.g., `"03:00"`).
* `radarr_url` / `sonarr_url` *(string)*: Network endpoints for your Radarr and Sonarr instances (e.g., `http://192.168.0.102:7878`).
* `radarr_api_key` / `sonarr_api_key` *(string)*: API authentication keys from your respective Arr instances.
* `processed_tags` *(array of strings)*: Tags applied to media files or tracked in the database once successfully processed (e.g., `["AsyncTranscoderr", "TdarrProcessed", "HEVC_Processed"]`).
* `global_source_paths` *(array of strings)*: Root directories scanned by default if no specific path overrides are defined.
* `allowed_extensions` *(array of strings)*: File extensions permitted for scanning and processing (e.g., `[".mp4", ".mkv", ".avi", ".webm", ".mpeg", ".m4v", ".ogm", ".mov", ".ts", ".zip"]`).
* `skip_terms` *(array of strings)*: Substring filters in filenames or paths that trigger immediate skipping (e.g., `"TELESYNC"`, `"DKS"`, `"WWE"`).
* `srt_clean_terms` *(array of strings)*: Junk strings and promotional watermarks automatically stripped out of sidecar subtitle files (`.srt`) during processing (e.g., `"Downloaded from"`, `"Official YIFY"`).

---

## 2. Multi-Node Worker Architectures & Exact Path Mapping Rules

`AsyncTranscoderr` supports distributed transcoding across multiple worker nodes. Depending on whether your worker node is **Local** (stable storage/direct mounts) or **Remote** (unstable networks, Tailscale, Wi-Fi, or NAS), the staging paths between the server config and the node config must align precisely.

### Scenario A: Local Node Setup (`local_mode: true`)
Use this when processing occurs directly on stable local storage or reliable mounts where network thrashing is not a concern.
* **Server Setup (`servers` array):**
  * `path_2a` & `path_2b`: **Must be identical** (e.g., `/path/to/media/processing1`) since no separate network grab stage is required.
  * `path_2c`: Completed output directory.
  * `path_failed`: Quarantine directory for errors.
  * `path_unmatched`: Fallback directory for unmatched files.
* **Corresponding Local Node Setup (`config_node_sa.json`):**
  * `path_process`: **Must match** `path_2a`/`path_2b` on the server.
  * `path_stage_grab`, `path_stage_completed`, `path_stage_failed`: **Must be blank (`""`)**.
  * `path_final_completed`: **Must match** `path_2c` on the server.
  * `path_final_failed`: **Must match** `path_failed` on the server.

### Scenario B: Remote / Distributed Node Setup (`local_mode: false`)
Use this when running on remote hardware where you want to stage files locally on fast scratch storage (NVMe/SSD) before pushing them back, preventing network share thrashing during FFmpeg encoding.
* **Server Setup (`servers` array):**
  * `path_2a`: Initial incoming watch directory (must match node's `path_monitor`).
  * `path_2b`: Active transcode working directory (must match node's `path_process`).
  * `path_2c`: Completed output directory (must match node's `path_final_completed`).
  * `path_failed`: Quarantine directory (must match node's `path_final_failed`).
  * `path_unmatched`: Fallback directory for unmatched files.
* **Corresponding Remote Node Setup (`config_node_sa.json`):**
  * `path_monitor`: **Must match** `path_2a` on the server.
  * `path_stage_grab`: Local node staging directory for incoming files.
  * `path_process`: **Must match** `path_2b` on the server.
  * `path_stage_completed`: Local node staging directory for completed files.
  * `path_final_completed`: **Must match** `path_2c` on the server.
  * `path_stage_failed`: Local node staging directory for failed files.
  * `path_final_failed`: **Must match** `path_failed` on the server.

---

## 3. Filtering & Integration Rules (`config.json`)

* `default_filter_config`: Baseline fallback rules applied across all media:
  * `skip_if_tagged`: Array of tags that bypass processing if present.
  * `skip_codecs`: Codecs to ignore (e.g., `["hevc", "h265"]`).
  * `min_size_bytes` / `max_size_bytes`: File size boundaries for processing.
  * `min_audio_channels`: Minimum audio channel threshold.
  * `min_height` / `max_height` / `min_width` / `max_width`: Resolution boundary constraints.
  * `modified_after_timestamp` / `modified_before_timestamp`: Date range constraints.
* `path_filter_rules`: Granular filter configurations mapped to specific directory paths.
* `media_profile_rules`: Syncs processing actions with specific Radarr/Sonarr quality profiles and target paths.
* `sonarr_unmonitor_rules`: Automatically unmonitors completed episodes in Sonarr once processing tags and completion statuses match.

---

## 4. Standalone Worker Node (`config_node_sa.json`) Specific Parameters

* `current_node_id` *(string)*: The unique identifier matching the corresponding server entry in `config.json` (e.g., `"server_1"`).
* `ffmpeg_bin` / `ffprobe_bin` *(string)*: Absolute paths to the binary executables for `ffmpeg` and `ffprobe`.
* `log_file_path` *(string)*: File path where node operation logs are saved (e.g., `"node_pipeline.log"`).
* `log_max_bytes` *(integer)*: Maximum size of log files before rotation occurs (e.g., `10485760` bytes / 10MB).
* `log_backup_count` *(integer)*: Number of rotated log backup files to retain.
* `check_already_tagged` *(string)*: Strategy for handling already tagged files (e.g., `"strip_and_retag"`).
* `tag_identifier` *(array of strings)*: List of tags checked to determine if a file has already been processed.
* `retag_identifier` *(string)*: The primary tag applied upon successful completion.
* `crf` *(integer/null)*: Constant Rate Factor for FFmpeg encoding (set to `null` to use default encoder settings).
* `preset` *(string)*: FFmpeg encoding speed preset (e.g., `""` for default or `"medium"`/`"slow"`).
* `audio_tracks` *(string)*: Audio track handling strategy (e.g., `"keep_english_fallback_all"`).
* `subtitle_tracks` *(string)*: Subtitle track filtering strategy (e.g., `"keep_english_and_forced"`).
* `extra_ffmpeg_args` *(array of strings)*: Additional custom command-line arguments passed directly to FFmpeg.
* `file_stability_wait_seconds` *(integer)*: Buffer time to wait and verify file size is stable before starting processing.
* `poll_interval_seconds` *(integer)*: Frequency at which the node checks directories for new tasks.
* `ffprobe_timeout_seconds` *(integer)*: Timeout threshold for running `ffprobe` analysis.
* `backoff_delays` *(array of integers)*: Exponential backoff delay intervals in seconds for handling failed tasks or connection retries.
* `high_priority_terms` *(array of strings)*: Filename terms or release groups given priority scheduling.
* `medium_priority_terms` *(array of strings)*: Secondary tier priority terms for queue ordering.
