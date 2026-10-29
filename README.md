# AsyncTranscoderr 🚀

A high-performance, hybrid media management and transcoding system built specifically for **Radarr** and **Sonarr** homelabs. Powered by a blazing-fast **Rust coordinator** leveraging Rayon multi-threading, AsyncTranscoderr has been rigorously tested and proven on massive scale paths containing **over 300,000 files (of which 130,000+ are large media files)**, delivering instant library scans and updates where traditional interpreted scripts grind to a halt.

Unlike traditional monolithic transcoders that choke when a network drops, AsyncTranscoderr is engineered from the ground up for **resilient distributed node execution** over flaky connections—whether you're running worker nodes over Wi-Fi, Tailscale tunnels, or remote links that bounce up and down. It pairs this high-performance Rust core with smart, automated Python nodes that feature **live real-time directory monitoring**, instant file processing, deep media cleanup, tag management, and quality profile automation.

## 🎯 Core Design Goals & Philosophy

AsyncTranscoderr was built from the ground up to solve specific homelab pain points through explicit engineering objectives:

1. **Absolute Truth via File State:** The physical file path, size, and modification timestamp serve as the ultimate source of truth. If a file shares the same path but its timestamp or size changes, it is instantly recognized as modified/uncompleted and re-evaluated against processing filters.

2. **"Set It and Forget It" Automation:** Real-time event tracking handles new or edited files automatically. Existing libraries are protected by smart date filters, key tags, and strict skip terms to prevent redundant or accidental reprocessing unless new configuration tags are explicitly introduced.

3. **Pristine Subtitles:** Automatically strips out annoying spam, promotional tags, and formatting watermarks from `.srt` subtitle files to eliminate on-screen annoyances.

4. **Seamless External Tool Interoperability:** Allows external tools to update or drop files automatically, having the system catch, process, and convert them without manual intervention.

5. **Smart Arr Integration (Auto-Unmonitoring):** Automatically instructs Radarr and Sonarr to stop upgrading or updating files once they have been successfully processed or already meet target criteria (e.g., 1080p with 5.1 audio).

6. **Maximum Storage Efficiency:** Drastically shrinks media library disk footprint using optimized HEVC (with AV1 future-proofing) while preserving visual fidelity.

7. **Distributed Node Resilience:** Leverages idle, low-powered, or remote worker nodes—even over unreliable connections like Wi-Fi or Tailscale tunnels—safely handling disconnects without state corruption.

8. **Universal Client Compatibility:** Standardizes all media files into identical codecs and restricted container types (`mkv`, `mp4`) so every client device can transcode and play files natively.

9. **Streamlined Audio & Subtitle Tracks:** Automatically strips out unwanted foreign audio tracks and redundant subtitle streams to keep files lean and clean.

## 🔥 Why AsyncTranscoderr?

* **Blazing-Fast Rust Library Scans:** Powered by Rayon multi-threading in Rust, the coordinator sweeps through massive media libraries (tested and proven on paths containing over 300,000 files, with 130,000+ large media files) instantly—leaving sluggish legacy Python/script-based scanners in the dust.

* **Live Real-Time File Monitoring:** Instantaneous event-driven processing. The moment a new file drops into your monitored path, the system catches it, verifies file stability, and kicks off processing without waiting for a scheduled scan.

* **Network Resilience for Remote Nodes:** Designed for distributed worker nodes. If your Tailscale tunnel drops or Wi-Fi blips, nodes handle connection backoffs gracefully, queue their work, and resume seamlessly without corrupting state or crashing.

* **Intelligent Media & Subtitle Cleanup:** Automatically strips out unwanted non-English audio tracks, removes unneeded subtitle clutter, and purges annoying spam, watermarks, or promotional tags from `.srt` subtitle files.

* **Advanced Filtering & Tagging:** Surgical control using skip terms, modification date ranges, codec matching, and automated **strip-and-retag** logic to ensure files are never redundantly processed.

* **Automated Profile & State Sync:** Dynamically updates Radarr/Sonarr quality profiles and automatically unmonitors completed series episodes once they are successfully optimized.

* **Effortless Docker Deployment:** Drop-in Docker and Docker Compose configuration gets your nodes up and running in seconds.

## 🏗️ System Architecture

* **Rust Coordinator (`media_manager`):** Manages SQLite database tracking, handles lightning-fast multi-threaded Rayon disk walks across 300k+ total / 130k+ media files, communicates with Radarr/Sonarr APIs, and orchestrates tasks across remote nodes.

* **Python Node Workers (`node_sa.py`):** Standalone, fault-tolerant worker agents that monitor input folders live, interface with FFmpeg for hardware/software accelerated encoding, and manage automated file handoffs.

* **Arr Sync Service (`arr_sync_service.py`):** Targeted or global sync helper to align quality profiles, update tags, and clean up library states.

## 🐳 Quick Start (Docker Compose)

1. **Clone the repository:**

   ```bash
   git clone https://github.com/vortex91/AsyncTranscoderr.git
   cd AsyncTranscoderr
   ```

2. **Set up your configuration files:**

   ```bash
   cp config_example.json config.json
   cp config_node_sa_example.json config_node_sa.json
   ```

   *Open both files in a text editor and plug in your Radarr/Sonarr API keys, URLs, and storage paths.*

3. **Launch the stack:**

   ```bash
   docker compose up -d
   ```

## ⚙️ Configuration & Feature Deep Dive

### 1. Global & Path-Based Filters (`config.json`)

* **Skip Terms:** Instantly bypass junk releases, telesyncs, sports broadcasts, or specific unwanted release groups (`"skip_terms": ["TELESYNC", "DKS", "WWE"]`).

* **Timestamp Filtering:** Target files within specific windows using `modified_after_timestamp` and `modified_before_timestamp` parameters.

* **Codec & Resolution Rules:** Define exact minimum/maximum audio channels, widths, heights, and target codecs to isolate files that actually need re-encoding to HEVC.

### 2. Node Execution & Live Monitoring (`config_node_sa_example.json`)

* **Live Directory Tracking (`path_monitor`):** Continuously watches incoming directories for new files, incorporating file-stability checks to ensure downloads are fully written before encoding starts.

* **Strip and Retag Logic (`check_already_tagged`):** Automatically sweeps legacy tags, injects success tags (e.g., `AsyncTranscoderr`, `TdarrProcessed`, `HEVC_Processed`), and prevents infinite processing loops.

* **Audio & Subtitle Policies:** Force-keep specific audio tracks (like English fallback) and clean foreign or redundant subtitle streams.

### 3. Arr Integration (`arr_sync_service.py`)

* **Quality Profile Updating:** Automatically shifts items into optimized quality profiles (e.g., "no upgrade") once processed.

* **Episode Unmonitoring:** Keeps your Sonarr lists clean by automatically unmonitoring completed series episodes that have already been transcoded and tagged.

## 🚧 Roadmap & What's Next

AsyncTranscoderr is actively expanding! Here is what is currently being worked on, and where contributions, feature requests, or bug reports are most welcome:

1. **Dual-Configuration Web GUI Integration:** Introducing a lightweight dashboard configuration interface offered *alongside* the traditional `config.json` as a flexible deployment option for users who prefer visual management without losing flat-file control.

2. **Advanced Filtering & Arr Controls:** Expanding fine-grained filtering options for custom conversion rules, alongside deeper dynamic handling of Radarr/Sonarr quality profiles and automated monitor/unmonitor states.

If you encounter a bug, have an idea for a new feature, or want to help build out the GUI, please feel free to **open an issue or submit a pull request** on the GitHub repository!

## 📄 License

This project is open-source and available under the [MIT License](LICENSE).

## ☕ Support & Donations

If AsyncTranscoderr has streamlined your homelab storage and kept your distributed nodes humming along smoothly, consider supporting its development:

<a href="https://buymeacoffee.com/vortex91" target="_blank">
  <img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" style="height: 60px !important;width: 217px !important;" >
</a>

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


## ☕ Support & Donations

If AsyncTranscoderr has streamlined your homelab storage and kept your distributed nodes humming along smoothly, consider supporting its development:

<a href="https://buymeacoffee.com/vortex91" target="_blank">
  <img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" style="height: 60px !important;width: 217px !important;" >
</a>

