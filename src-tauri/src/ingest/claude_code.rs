//! Claude Code ingester.
//!
//! Sources: <claude-home>/projects/<encoded-cwd>/*.jsonl and .../subagents/agent-*.jsonl
//! where <claude-home> is `~/.claude` plus every extra directory the user added in
//! Settings (multi-account: each `CLAUDE_CONFIG_DIR` has its own projects/ tree).
//! Strategy: initial backfill walk, then `notify` watcher → per-file byte-offset tail.
//! Every write batch commits events + new offset in ONE transaction (restart-safe,
//! duplicate-safe via the uq_events_source index).
//!
//! Schema-tolerance rules (do not "improve" these away):
//!   * parse into serde_json::Value, never into rigid structs
//!   * missing/renamed fields → best-effort extraction, never an error
//!   * a line that matches nothing → single EventKind::Unknown with raw preserved
//!   * only complete lines (ending '\n') are consumed; the tail remainder stays
//!     un-offset so a partially-flushed line is re-read next round

use crate::normalize::*;
use crate::store::Store;
use anyhow::{Context, Result};
use notify::{RecursiveMode, Watcher};
use serde_json::Value;
use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Flush pending file-change notifications no more often than this (debounce).
const FLUSH_INTERVAL: Duration = Duration::from_millis(200);
/// Reconciliation sweep cadence — catches notify events the OS dropped.
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Every transcript root to ingest: `<claude-home>/projects` for the default
/// home and each extra directory configured in Settings, existence-filtered.
///
/// Mirrors the multi-root shape already used by `skills_config::read_all` —
/// discovery here, the walk in a function that takes explicit roots, so both
/// stay testable against a temp tree.
pub fn claude_projects_dirs(store: &Store) -> Vec<PathBuf> {
    projects_dirs_of(&store.claude_home_dirs())
}

/// Map Claude home directories to their `projects/` subdirectory, dropping any
/// that doesn't exist. Split out from [`claude_projects_dirs`] for testing.
fn projects_dirs_of(homes: &[PathBuf]) -> Vec<PathBuf> {
    homes
        .iter()
        .map(|h| h.join("projects"))
        .filter(|p| p.is_dir())
        .collect()
}

/// Blocking entry point. Run on a dedicated thread (keeps the watcher alive):
/// initial backfill, then watch + debounced tail + periodic reconciliation sweep.
///
/// The set of watched roots is re-read whenever Settings change, so adding or
/// removing a Claude directory takes effect without restarting the app. The loop
/// is entered even when no root exists yet — otherwise a directory added later
/// would never be picked up.
pub fn run(store: Store) -> Result<()> {
    // 1. Backfill with progress reporting so the window fills in as it runs.
    let n = backfill(&store, true)?;
    tracing::info!(files = n, "claude_code backfill complete");
    let _ = store.enforce_retention();
    let _ = store.reconcile_source_alive();
    // Flip the banner to the steady "watching" state.
    store.emit_progress(crate::store::IngestProgress {
        phase: "watching".into(),
        files_done: n,
        files_total: n,
        events: 0,
        done: true,
    });

    // 2. Watch recursively; the callback runs on notify's thread → forward paths.
    let (tx, rx) = std::sync::mpsc::channel::<notify::Result<notify::Event>>();
    let mut watcher = notify::recommended_watcher(move |res| {
        let _ = tx.send(res);
    })
    .context("create fs watcher")?;
    // One watcher, N watched roots (notify supports repeated watch/unwatch).
    let mut watched: HashSet<PathBuf> = HashSet::new();
    sync_watched_roots(&mut watcher, &mut watched, &claude_projects_dirs(&store));
    let mut settings_gen = store.settings_gen();

    // 3. Drain loop: coalesce changes, flush ≤ every FLUSH_INTERVAL, sweep every 30s.
    let mut pending: HashSet<PathBuf> = HashSet::new();
    let mut last_flush = Instant::now();
    let mut last_sweep = Instant::now();
    loop {
        match rx.recv_timeout(FLUSH_INTERVAL) {
            Ok(Ok(event)) => {
                for p in event.paths {
                    if is_jsonl(&p) {
                        pending.insert(p);
                    }
                }
            }
            Ok(Err(e)) => tracing::warn!("watch error: {e}"),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }

        // Settings changed → apply the new watch list right away. One relaxed
        // atomic load per tick, so this is free when nothing changed.
        let gen = store.settings_gen();
        if gen != settings_gen {
            settings_gen = gen;
            let roots = claude_projects_dirs(&store);
            let added = sync_watched_roots(&mut watcher, &mut watched, &roots);
            if !added.is_empty() {
                // Backfill only what's new — re-walking every root would stall
                // the loop for minutes on a large archive. Live tailing pauses
                // for the duration; notify's channel is unbounded, so the events
                // that arrive meanwhile are drained on the next iteration.
                let files = match backfill_roots(&store, &added, true) {
                    Ok(files) => {
                        tracing::info!(roots = added.len(), files, "backfilled added roots");
                        files
                    }
                    Err(e) => {
                        tracing::warn!("backfill of added roots failed: {e:#}");
                        0
                    }
                };
                let _ = store.enforce_retention();
                let _ = store.reconcile_source_alive();
                store.emit_sessions_updated();
                // Back to the steady state (also clears the Settings spinner).
                store.emit_progress(crate::store::IngestProgress {
                    phase: "watching".into(),
                    files_done: files,
                    files_total: files,
                    events: 0,
                    done: true,
                });
            }
        }

        if !pending.is_empty() && last_flush.elapsed() >= FLUSH_INTERVAL {
            for path in pending.drain() {
                if let Err(e) = tail_file(&store, &path, true) {
                    tracing::warn!(path = %path.display(), "live tail failed: {e:#}");
                }
            }
            last_flush = Instant::now();
        }

        if last_sweep.elapsed() >= SWEEP_INTERVAL {
            // backfill() re-tails every file from its stored offset → picks up
            // any change the watcher missed (atomic writes, dropped events).
            // Silent (report=false): the sweep must not spam progress events.
            // It also re-syncs the watch list, so a configured directory that
            // only just appeared on disk starts being watched.
            let roots = claude_projects_dirs(&store);
            sync_watched_roots(&mut watcher, &mut watched, &roots);
            if let Err(e) = backfill_roots(&store, &roots, false) {
                tracing::warn!("reconciliation sweep failed: {e:#}");
            }
            let _ = store.enforce_retention();
            let _ = store.reconcile_source_alive();
            last_sweep = Instant::now();
        }
    }
    Ok(())
}

/// Bring the watcher in line with `desired`: watch what's new, unwatch what's
/// gone. Returns the roots that were newly watched (the caller backfills those).
/// Never fails the loop — a root that can't be watched is logged and skipped, so
/// one bad directory can't take the whole ingester down.
fn sync_watched_roots(
    watcher: &mut dyn Watcher,
    watched: &mut HashSet<PathBuf>,
    desired: &[PathBuf],
) -> Vec<PathBuf> {
    let want: HashSet<PathBuf> = desired.iter().cloned().collect();
    for stale in watched.difference(&want).cloned().collect::<Vec<_>>() {
        if let Err(e) = watcher.unwatch(&stale) {
            tracing::warn!(path = %stale.display(), "unwatch failed: {e}");
        }
        watched.remove(&stale);
        tracing::info!(path = %stale.display(), "stopped watching claude projects");
    }
    let mut added = Vec::new();
    for root in desired {
        if watched.contains(root) {
            continue;
        }
        match watcher.watch(root, RecursiveMode::Recursive) {
            Ok(()) => {
                watched.insert(root.clone());
                added.push(root.clone());
                tracing::info!(path = %root.display(), "watching claude projects");
            }
            Err(e) => tracing::warn!(path = %root.display(), "watch failed: {e}"),
        }
    }
    added
}

fn is_jsonl(p: &Path) -> bool {
    p.extension().and_then(|e| e.to_str()) == Some("jsonl")
}

/// Backfill every configured root: walk all *.jsonl (incl. subagents/) and tail
/// each from its stored offset (0 on first run). Individual events aren't pushed
/// to the timeline, but when `report` is set, a throttled progress signal +
/// periodic list refresh let the UI show the archive filling in. Returns number
/// of files touched.
pub fn backfill(store: &Store, report: bool) -> Result<usize> {
    backfill_roots(store, &claude_projects_dirs(store), report)
}

/// Backfill exactly the given roots. Used by [`backfill`] for a full pass and by
/// the watch loop to backfill a single directory the user just added.
pub fn backfill_roots(store: &Store, roots: &[PathBuf], report: bool) -> Result<usize> {
    let limit = store.backfill_file_limit();
    let mut files: Vec<PathBuf> = Vec::new();
    for root in roots {
        files.extend(root_files(root, limit)?);
    }
    let total = files.len();

    let mut events: i64 = 0;
    let mut last_report = Instant::now();
    let mut last_list = Instant::now();
    for (i, entry) in files.iter().enumerate() {
        match tail_file(store, entry, false) {
            Ok(n) => events += n as i64,
            Err(e) => tracing::warn!(path = %entry.display(), "backfill tail failed: {e:#}"),
        }
        if report {
            // Throttle progress to ~7/s and list refetches to ~1/s so the window
            // populates during a multi-minute first backfill without flooding.
            if last_report.elapsed() >= Duration::from_millis(150) {
                store.emit_progress(crate::store::IngestProgress {
                    phase: "backfilling".into(),
                    files_done: i + 1,
                    files_total: total,
                    events,
                    done: false,
                });
                last_report = Instant::now();
            }
            if last_list.elapsed() >= Duration::from_secs(1) {
                store.emit_sessions_updated();
                last_list = Instant::now();
            }
        }
    }
    if report {
        store.emit_progress(crate::store::IngestProgress {
            phase: "backfilling".into(),
            files_done: total,
            files_total: total,
            events,
            done: true,
        });
        store.emit_sessions_updated();
    }
    Ok(total)
}

/// The transcript files of ONE root, newest first, capped at `limit`.
///
/// The cap is applied per root so a busy account can't starve the others, and
/// the sort makes it deterministic: `walkdir` returns files in stack-pop order,
/// so a plain truncate kept an arbitrary subset. mtime is read best-effort — an
/// unreadable entry sorts last rather than aborting the walk.
fn root_files(root: &Path, limit: Option<usize>) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = walkdir(root)?.into_iter().filter(|p| is_jsonl(p)).collect();
    if let Some(limit) = limit {
        if files.len() > limit {
            files.sort_by_key(|p| {
                std::cmp::Reverse(
                    std::fs::metadata(p)
                        .and_then(|m| m.modified())
                        .unwrap_or(std::time::UNIX_EPOCH),
                )
            });
            files.truncate(limit);
        }
    }
    Ok(files)
}

/// Tail one JSONL file from its persisted byte offset. Idempotent; safe to call
/// on every notify event AND from the reconciliation sweep. When `emit` is true,
/// newly-inserted events are pushed to the frontend after the commit. Returns the
/// number of events inserted.
pub fn tail_file(store: &Store, path: &Path, emit: bool) -> Result<usize> {
    let source = path.to_string_lossy().to_string();
    let mut offset = store.get_offset(&source)?;

    let mut f = std::fs::File::open(path).with_context(|| format!("open {source}"))?;
    let len = f.metadata()?.len();
    if len < offset {
        // File truncated/rotated (shouldn't happen for cc, but never assume): restart.
        tracing::warn!(path = %source, "file shrank ({len} < {offset}), re-reading");
        offset = 0;
    }
    if len == offset {
        return Ok(0);
    }

    f.seek(SeekFrom::Start(offset))?;
    let mut buf = Vec::with_capacity((len - offset) as usize);
    f.read_to_end(&mut buf)?;

    // Consume only complete lines; a partially-flushed tail is re-read next round.
    let consumed = match buf.iter().rposition(|&b| b == b'\n') {
        Some(last_nl) => last_nl + 1,
        None => return Ok(0), // no complete line yet
    };
    let chunk = &buf[..consumed];
    let is_sidechain_file = source.contains("/subagents/");

    let mut batches = Vec::new();
    for line in chunk.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let raw = String::from_utf8_lossy(line).to_string();
        batches.push(normalize_line(&raw, path, is_sidechain_file));
    }

    // events + offset in one transaction — offset and data can never disagree.
    let inserted = store.commit_batches(&source, offset + consumed as u64, batches)?;
    let n = inserted.len();
    if emit {
        store.emit_appended(inserted);
    }
    Ok(n)
}

/// Normalize one transcript line. NEVER returns Err — worst case is Unknown.
pub fn normalize_line(raw: &str, path: &Path, sidechain_file: bool) -> NormalizedBatch {
    let mut out = NormalizedBatch::default();
    let v: Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => {
            out.events.push(unknown_event(fallback_session_id(path), raw));
            return out;
        }
    };

    let s = |k: &str| v.get(k).and_then(Value::as_str).map(str::to_string);
    // Sidechain (subagent) transcripts are their own session, keyed by the FILE.
    // Their `sessionId` field frequently points at the PARENT session — using it
    // would merge subagent events into the parent and flip the parent to
    // is_subagent. Main transcripts key by sessionId as usual.
    let native_sid = if sidechain_file {
        stem(path)
    } else {
        s("sessionId").unwrap_or_else(|| stem(path))
    };
    let session_id = format!("cc:{native_sid}");
    // A sidechain's `sessionId` is its PARENT (the main conversation it belongs
    // to) — a real hard link, not a heuristic. Record it so the UI shows true
    // parent → child, not time-overlap siblings.
    let parent_session_id = if sidechain_file {
        s("sessionId")
            .map(|p| format!("cc:{p}"))
            .filter(|p| p != &session_id)
    } else {
        None
    };
    let ts = s("timestamp");
    let uuid = s("uuid");
    let parent_uuid = s("parentUuid");
    let is_sidechain =
        sidechain_file || v.get("isSidechain").and_then(Value::as_bool).unwrap_or(false);

    // Session metadata (merge-upserted; last non-None wins).
    out.session = Some(NormalizedSession {
        id: session_id.clone(),
        agent: AgentKind::ClaudeCode,
        project_path: s("cwd"),
        title: None, // set by store from first User event text
        model: v
            .pointer("/message/model")
            .and_then(Value::as_str)
            .map(str::to_string),
        git_branch: s("gitBranch"),
        started_at: ts.clone(),
        updated_at: ts.clone(),
        is_subagent: is_sidechain,
        parent_session_id,
        source_ref: Some(path.to_string_lossy().to_string()),
    });

    let line_type = s("type").unwrap_or_default();
    match line_type.as_str() {
        "user" | "assistant" => {
            let role = v
                .pointer("/message/role")
                .and_then(Value::as_str)
                .unwrap_or(&line_type);
            let (tin, tout) = usage(&v);
            let content = v.pointer("/message/content");
            match content {
                // Plain-string user content
                Some(Value::String(text)) => out.events.push(NormalizedEvent {
                    session_id: session_id.clone(),
                    ts: ts.clone(),
                    kind: if role == "user" {
                        EventKind::User
                    } else {
                        EventKind::Assistant
                    },
                    role: Some(role.into()),
                    text: Some(text.clone()),
                    tool_name: None,
                    tool_input_json: None,
                    tool_result_json: None,
                    tokens_in: tin,
                    tokens_out: tout,
                    source_uuid: uuid.clone(),
                    parent_uuid: parent_uuid.clone(),
                    tool_use_id: None,
                    raw_json: raw.into(),
                }),
                // Block array: text / thinking / tool_use / tool_result → 1 event each
                Some(Value::Array(blocks)) => {
                    for (i, b) in blocks.iter().enumerate() {
                        let bt = b.get("type").and_then(Value::as_str).unwrap_or("");
                        let (kind, text, tool_name, tool_input, tool_result) = match bt {
                            "text" => (
                                if role == "user" {
                                    EventKind::User
                                } else {
                                    EventKind::Assistant
                                },
                                b.get("text").and_then(Value::as_str).map(str::to_string),
                                None,
                                None,
                                None,
                            ),
                            "thinking" => (
                                EventKind::Thinking,
                                b.get("thinking").and_then(Value::as_str).map(str::to_string),
                                None,
                                None,
                                None,
                            ),
                            "tool_use" => (
                                EventKind::ToolCall,
                                None,
                                b.get("name").and_then(Value::as_str).map(str::to_string),
                                b.get("input").map(|x| x.to_string()),
                                None,
                            ),
                            "tool_result" => (
                                EventKind::ToolResult,
                                None,
                                None,
                                None,
                                b.get("content").map(|x| x.to_string()),
                            ),
                            "image" => (
                                EventKind::Meta,
                                Some("image".to_string()),
                                None,
                                None,
                                None,
                            ),
                            _ => (EventKind::Unknown, None, None, None, None),
                        };
                        // Correlation id: a tool_use carries its own `id`; the
                        // matching tool_result carries `tool_use_id`.
                        let tool_use_id = match bt {
                            "tool_use" => b.get("id").and_then(Value::as_str).map(str::to_string),
                            "tool_result" => {
                                b.get("tool_use_id").and_then(Value::as_str).map(str::to_string)
                            }
                            _ => None,
                        };
                        out.events.push(NormalizedEvent {
                            session_id: session_id.clone(),
                            ts: ts.clone(),
                            kind,
                            role: Some(role.into()),
                            text,
                            tool_name,
                            tool_input_json: tool_input,
                            tool_result_json: tool_result,
                            // usage belongs to the message; attach to first block only
                            tokens_in: if i == 0 { tin } else { None },
                            tokens_out: if i == 0 { tout } else { None },
                            // uuid must stay unique per event for the dedupe index
                            source_uuid: uuid.as_ref().map(|u| format!("{u}#{i}")),
                            parent_uuid: parent_uuid.clone(),
                            tool_use_id,
                            raw_json: raw.into(),
                        });
                    }
                }
                _ => out.events.push(unknown_event(session_id.clone(), raw)),
            }
        }
        "summary" => out.events.push(NormalizedEvent {
            session_id: session_id.clone(),
            ts,
            kind: EventKind::Summary,
            role: None,
            text: s("summary"),
            tool_name: None,
            tool_input_json: None,
            tool_result_json: None,
            tokens_in: None,
            tokens_out: None,
            source_uuid: uuid,
            parent_uuid,
            tool_use_id: None,
            raw_json: raw.into(),
        }),
        "system" => out.events.push(NormalizedEvent {
            session_id: session_id.clone(),
            ts,
            kind: EventKind::System,
            role: None,
            text: s("content").or_else(|| s("subtype")),
            tool_name: None,
            tool_input_json: None,
            tool_result_json: None,
            tokens_in: None,
            tokens_out: None,
            source_uuid: uuid,
            parent_uuid,
            tool_use_id: None,
            raw_json: raw.into(),
        }),
        // ai-title carries the human-readable session title — the best title we
        // have. Set it on the session; emit no timeline event.
        "ai-title" => {
            if let (Some(sess), Some(t)) = (out.session.as_mut(), s("aiTitle")) {
                sess.title = Some(t);
            }
        }
        // pr-link is genuinely useful — surface it as a system event with a
        // clickable link (GitLab merge requests vs GitHub pull requests).
        "pr-link" => {
            let n = v.get("prNumber").and_then(|x| x.as_i64());
            let repo = s("prRepository").unwrap_or_default();
            let url = s("prUrl").unwrap_or_default();
            let kind = if url.contains("/merge_requests/") { "MR" } else { "PR" };
            let label = match n {
                Some(n) => format!("{kind} #{n} · {repo}"),
                None => format!("{kind} · {repo}"),
            };
            // Markdown link when we have a URL → the UI renders it clickable.
            let text = if url.is_empty() {
                label
            } else {
                format!("[{label}]({url})")
            };
            out.events.push(NormalizedEvent {
                session_id: session_id.clone(),
                ts,
                kind: EventKind::System,
                role: None,
                text: Some(text),
                tool_name: None,
                tool_input_json: None,
                tool_result_json: None,
                tokens_in: None,
                tokens_out: None,
                source_uuid: uuid,
                parent_uuid,
                tool_use_id: None,
                raw_json: raw.into(),
            });
        }
        // Known control/metadata lines → Meta (hidden by default in the UI).
        "mode" | "permission-mode" | "queue-operation" | "attachment"
        | "file-history-snapshot" | "last-prompt" | "bridge-session" => {
            out.events.push(meta_event(session_id.clone(), ts, meta_label(&line_type, &v), raw));
        }
        _ => out.events.push(unknown_event(session_id.clone(), raw)),
    }
    out
}

/// Concise human label for a known control line. Eridian is a review tool, so
/// these should say *what actually happened*, not just the line's category —
/// especially `attachment`, which fans out into ~20 distinct kinds.
fn meta_label(line_type: &str, v: &Value) -> String {
    let field = |k: &str| v.get(k).and_then(Value::as_str).unwrap_or("");
    match line_type {
        "mode" => format!("mode: {}", field("mode")),
        "permission-mode" => format!("permission: {}", field("permissionMode")),
        "queue-operation" => format!("queued: {}", field("operation")),
        "file-history-snapshot" => "file snapshot".to_string(),
        "attachment" => attachment_label(v),
        "bridge-session" => {
            let id = short_id(field("bridgeSessionId"));
            if id.is_empty() {
                "bridge session".to_string()
            } else {
                format!("bridge session · {id}")
            }
        }
        "last-prompt" => "last prompt".to_string(),
        other => other.to_string(),
    }
}

/// First 8 chars of an id (enough to correlate without the full noise).
fn short_id(id: &str) -> String {
    id.chars().take(8).collect()
}

/// Label an `attachment` control line by its inner `attachment.type`, appending a
/// concise, type-specific detail (path, hook name, counts) when one is present.
/// Defensive: an unknown/missing type still yields a useful, non-empty label.
fn attachment_label(v: &Value) -> String {
    let a = v.get("attachment");
    let s = |k: &str| {
        a.and_then(|a| a.get(k))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    };
    let count = |k: &str| a.and_then(|a| a.get(k)).and_then(Value::as_i64);
    let arr = |k: &str| {
        a.and_then(|a| a.get(k))
            .and_then(Value::as_array)
            .map(|x| x.len())
    };
    let path = || {
        let p = s("displayPath");
        if !p.is_empty() {
            return p;
        }
        let f = s("filename");
        if !f.is_empty() {
            return f;
        }
        s("path")
    };

    let ty = s("type");
    if ty.is_empty() {
        return "attachment".to_string();
    }
    let detail = match ty.as_str() {
        "hook_success" | "hook_cancelled" | "hook_additional_context"
        | "hook_system_message" => {
            let hook = {
                let n = s("hookName");
                if n.is_empty() {
                    s("hookEvent")
                } else {
                    n
                }
            };
            if hook.is_empty() {
                String::new()
            } else {
                format!(" · {hook}")
            }
        }
        "file" | "edited_text_file" | "already_read_file" | "directory"
        | "compact_file_reference" => {
            let p = path();
            if p.is_empty() {
                String::new()
            } else {
                format!(" · {p}")
            }
        }
        "skill_listing" => count("skillCount")
            .map(|c| format!(" · {c}"))
            .unwrap_or_default(),
        "invoked_skills" => arr("skills").map(|c| format!(" · {c}")).unwrap_or_default(),
        "deferred_tools_delta" => {
            let add = arr("addedNames").unwrap_or(0);
            let rem = arr("removedNames").unwrap_or(0);
            if add + rem > 0 {
                format!(" · +{add}/-{rem}")
            } else {
                String::new()
            }
        }
        "command_permissions" => count("itemCount")
            .map(|c| format!(" · {c}"))
            .unwrap_or_default(),
        "date_change" => {
            let d = s("newDate");
            if d.is_empty() {
                String::new()
            } else {
                format!(" · {d}")
            }
        }
        _ => String::new(),
    };
    format!("attachment · {ty}{detail}")
}

fn meta_event(session_id: String, ts: Option<String>, text: String, raw: &str) -> NormalizedEvent {
    NormalizedEvent {
        session_id,
        ts,
        kind: EventKind::Meta,
        role: None,
        text: Some(text),
        tool_name: None,
        tool_input_json: None,
        tool_result_json: None,
        tokens_in: None,
        tokens_out: None,
        source_uuid: None,
        parent_uuid: None,
        tool_use_id: None,
        raw_json: raw.into(),
    }
}

fn usage(v: &Value) -> (Option<i64>, Option<i64>) {
    // Input side = the whole prompt actually sent: fresh input + cache reads +
    // cache creation. Cache tokens dominate a real Claude Code turn, so counting
    // only input_tokens would badly under-report both cost and context fill.
    let u = v.pointer("/message/usage");
    let field = |k: &str| u.and_then(|u| u.get(k)).and_then(Value::as_i64);
    let input = field("input_tokens");
    let cache_read = field("cache_read_input_tokens");
    let cache_create = field("cache_creation_input_tokens");
    let total_in = match (input, cache_read, cache_create) {
        (None, None, None) => None,
        _ => Some(input.unwrap_or(0) + cache_read.unwrap_or(0) + cache_create.unwrap_or(0)),
    };
    (total_in, field("output_tokens"))
}

fn unknown_event(session_id: String, raw: &str) -> NormalizedEvent {
    NormalizedEvent {
        session_id,
        ts: None,
        kind: EventKind::Unknown,
        role: None,
        text: None,
        tool_name: None,
        tool_input_json: None,
        tool_result_json: None,
        tokens_in: None,
        tokens_out: None,
        source_uuid: None,
        parent_uuid: None,
        tool_use_id: None,
        raw_json: raw.into(),
    }
}

fn stem(p: &Path) -> String {
    p.file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".into())
}
fn fallback_session_id(p: &Path) -> String {
    format!("cc:{}", stem(p))
}

/// Minimal recursive walk.
fn walkdir(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) => {
                tracing::warn!(path = %dir.display(), "read_dir failed: {e}");
                continue;
            }
        };
        for e in rd {
            let p = match e {
                Ok(e) => e.path(),
                Err(_) => continue,
            };
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push(p);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p() -> PathBuf {
        PathBuf::from("/home/u/.claude/projects/proj/s1.jsonl")
    }

    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    /// Build a throwaway tree of fake Claude homes:
    /// `<tmp>/<name>/projects/proj/s<i>.jsonl`, one transcript line per file.
    fn temp_homes(names: &[&str], files_per_home: usize) -> (PathBuf, Vec<PathBuf>) {
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let base = std::env::temp_dir().join(format!("eridian_cc_{}_{}", std::process::id(), n));
        let mut homes = Vec::new();
        for name in names {
            let home = base.join(name);
            let proj = home.join("projects").join("proj");
            std::fs::create_dir_all(&proj).unwrap();
            for i in 0..files_per_home {
                // Session ids must stay globally unique — a real multi-account
                // setup never shares a UUID between homes.
                let sid = format!("{name}-{i}");
                std::fs::write(
                    proj.join(format!("{sid}.jsonl")),
                    format!(
                        r#"{{"type":"user","sessionId":"{sid}","uuid":"u-{sid}","timestamp":"2026-08-08T00:00:00Z","cwd":"/work/proj","message":{{"role":"user","content":"hi"}}}}"#
                    ) + "\n",
                )
                .unwrap();
            }
            homes.push(home);
        }
        (base, homes)
    }

    #[test]
    fn sync_watched_roots_adds_removes_and_reports_only_the_new() {
        // This is the live-apply mechanism: on a Settings change the loop diffs
        // the desired roots against what it already watches and backfills only
        // the additions (re-walking everything would stall it for minutes).
        let (base, homes) = temp_homes(&[".claude", ".claude-alpha"], 1);
        let roots = projects_dirs_of(&homes);
        let (tx, _rx) = std::sync::mpsc::channel::<notify::Result<notify::Event>>();
        let mut watcher = notify::recommended_watcher(move |res| {
            let _ = tx.send(res);
        })
        .unwrap();
        let mut watched: HashSet<PathBuf> = HashSet::new();

        let added = sync_watched_roots(&mut watcher, &mut watched, &roots);
        assert_eq!(added.len(), 2, "both roots are new on the first sync");
        assert_eq!(watched.len(), 2);

        // Idempotent: re-syncing the same list watches nothing again, so the
        // caller doesn't re-backfill on every tick.
        assert!(sync_watched_roots(&mut watcher, &mut watched, &roots).is_empty());

        // Adding one root reports exactly that root.
        let mut watched_one: HashSet<PathBuf> = HashSet::new();
        let _ = sync_watched_roots(&mut watcher, &mut watched_one, &roots[..1]);
        let added = sync_watched_roots(&mut watcher, &mut watched_one, &roots);
        assert_eq!(added, vec![roots[1].clone()]);

        // Removing a root unwatches it and reports no additions.
        let added = sync_watched_roots(&mut watcher, &mut watched, &roots[..1]);
        assert!(added.is_empty());
        assert_eq!(watched, HashSet::from([roots[0].clone()]));

        // A directory that can't be watched is skipped, not fatal.
        let bogus = vec![base.join("definitely-absent")];
        assert!(sync_watched_roots(&mut watcher, &mut watched, &bogus).is_empty());
        assert!(watched.is_empty(), "the old root was still unwatched");
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn projects_dirs_of_appends_projects_and_drops_missing() {
        let (base, homes) = temp_homes(&[".claude", ".claude-alpha"], 1);
        let mut with_missing = homes.clone();
        with_missing.push(base.join(".claude-gone"));
        let dirs = projects_dirs_of(&with_missing);
        assert_eq!(dirs.len(), 2, "the non-existent home must be skipped");
        assert!(dirs.iter().all(|d| d.ends_with("projects")));
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn backfill_roots_ingests_every_root() {
        let (base, homes) = temp_homes(&[".claude", ".claude-alpha", ".claude-beta"], 2);
        let store = Store::open_in_memory().unwrap();
        let roots = projects_dirs_of(&homes);
        let files = backfill_roots(&store, &roots, false).unwrap();
        assert_eq!(files, 6, "2 files x 3 roots");

        let sessions = store.list_sessions(None).unwrap();
        assert_eq!(sessions.len(), 6);
        // Each session carries the account label derived from its own root.
        let mut labels: Vec<Option<String>> =
            sessions.iter().map(|s| s.account.clone()).collect();
        labels.sort();
        labels.dedup();
        assert_eq!(
            labels,
            vec![None, Some("alpha".to_string()), Some("beta".to_string())],
            "default root unlabeled, extras labeled by directory name"
        );

        // Idempotent: a second pass (restart / sweep) adds nothing.
        backfill_roots(&store, &roots, false).unwrap();
        assert_eq!(store.list_sessions(None).unwrap().len(), 6);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn backfill_file_limit_applies_per_root_not_globally() {
        // A global truncate would let one busy account starve the others; the
        // cap is per root so every account keeps its most recent transcripts.
        let (base, homes) = temp_homes(&[".claude", ".claude-alpha"], 3);
        let roots = projects_dirs_of(&homes);
        for root in &roots {
            assert_eq!(root_files(root, Some(2)).unwrap().len(), 2);
            assert_eq!(root_files(root, None).unwrap().len(), 3);
            // A limit above the file count is a no-op.
            assert_eq!(root_files(root, Some(9)).unwrap().len(), 3);
        }
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn root_files_keeps_the_newest_when_capped() {
        // Truncation must be deterministic: walkdir returns files in stack-pop
        // order, so a plain truncate kept an arbitrary subset of the archive.
        let (base, homes) = temp_homes(&[".claude-alpha"], 3);
        let root = homes[0].join("projects");
        let mut all = root_files(&root, None).unwrap();
        all.sort();
        let target = all[0].clone();
        // Rewriting the file bumps its mtime — no extra dependency needed.
        std::thread::sleep(Duration::from_millis(50));
        let content = std::fs::read(&target).unwrap();
        std::fs::write(&target, content).unwrap();

        assert_eq!(
            root_files(&root, Some(1)).unwrap(),
            vec![target],
            "the most recently modified file survives the cap"
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn user_string_content_is_one_user_event() {
        let raw = r#"{"type":"user","sessionId":"s1","uuid":"u1","timestamp":"2026-08-08T00:00:00Z","cwd":"/proj","gitBranch":"main","message":{"role":"user","content":"hello"}}"#;
        let b = normalize_line(raw, &p(), false);
        assert_eq!(b.events.len(), 1);
        assert_eq!(b.events[0].kind, EventKind::User);
        assert_eq!(b.events[0].text.as_deref(), Some("hello"));
        let s = b.session.unwrap();
        assert_eq!(s.id, "cc:s1");
        assert_eq!(s.project_path.as_deref(), Some("/proj"));
        assert_eq!(s.git_branch.as_deref(), Some("main"));
        assert!(!s.is_subagent);
    }

    #[test]
    fn captures_tool_use_id_for_bash_call_and_result() {
        let call = r#"{"type":"assistant","sessionId":"s1","uuid":"a1","timestamp":"2026-08-11T00:00:00Z","message":{"role":"assistant","content":[{"type":"tool_use","id":"toolu_01","name":"Bash","input":{"command":"git status"}}]}}"#;
        let result = r#"{"type":"user","sessionId":"s1","uuid":"u1","timestamp":"2026-08-11T00:00:02Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_01","content":"ok"}]}}"#;
        let b1 = normalize_line(call, &p(), false);
        let call_ev = b1.events.iter().find(|e| e.kind == EventKind::ToolCall).unwrap();
        assert_eq!(call_ev.tool_use_id.as_deref(), Some("toolu_01"));
        let b2 = normalize_line(result, &p(), false);
        let res_ev = b2.events.iter().find(|e| e.kind == EventKind::ToolResult).unwrap();
        assert_eq!(res_ev.tool_use_id.as_deref(), Some("toolu_01"));
    }

    #[test]
    fn assistant_block_array_splits_into_events() {
        let raw = r#"{"type":"assistant","sessionId":"s1","uuid":"a1","timestamp":"2026-08-08T00:00:01Z","message":{"role":"assistant","model":"claude-x","usage":{"input_tokens":10,"output_tokens":20},"content":[{"type":"thinking","thinking":"hmm"},{"type":"text","text":"answer"},{"type":"tool_use","name":"Bash","input":{"command":"ls"}}]}}"#;
        let b = normalize_line(raw, &p(), false);
        assert_eq!(b.events.len(), 3);
        assert_eq!(b.events[0].kind, EventKind::Thinking);
        assert_eq!(b.events[0].text.as_deref(), Some("hmm"));
        // usage attaches to the first block only
        assert_eq!(b.events[0].tokens_in, Some(10));
        assert_eq!(b.events[1].tokens_in, None);
        assert_eq!(b.events[1].kind, EventKind::Assistant);
        assert_eq!(b.events[2].kind, EventKind::ToolCall);
        assert_eq!(b.events[2].tool_name.as_deref(), Some("Bash"));
        assert!(b.events[2].tool_input_json.as_deref().unwrap().contains("ls"));
        // per-block uuids are unique for the dedupe index
        assert_eq!(b.events[0].source_uuid.as_deref(), Some("a1#0"));
        assert_eq!(b.events[2].source_uuid.as_deref(), Some("a1#2"));
        assert_eq!(b.session.unwrap().model.as_deref(), Some("claude-x"));
    }

    #[test]
    fn tool_result_block_is_captured() {
        let raw = r#"{"type":"user","sessionId":"s1","uuid":"u2","message":{"role":"user","content":[{"type":"tool_result","content":"file listing"}]}}"#;
        let b = normalize_line(raw, &p(), false);
        assert_eq!(b.events.len(), 1);
        assert_eq!(b.events[0].kind, EventKind::ToolResult);
        assert!(b.events[0]
            .tool_result_json
            .as_deref()
            .unwrap()
            .contains("file listing"));
    }

    #[test]
    fn image_block_is_meta() {
        let raw = r#"{"type":"assistant","sessionId":"s1","uuid":"a2","message":{"role":"assistant","content":[{"type":"image","source":{}}]}}"#;
        let b = normalize_line(raw, &p(), false);
        assert_eq!(b.events.len(), 1);
        assert_eq!(b.events[0].kind, EventKind::Meta);
    }

    #[test]
    fn ai_title_sets_session_title_and_emits_no_event() {
        let raw = r#"{"type":"ai-title","aiTitle":"Refactor the ingest loop","sessionId":"s1"}"#;
        let b = normalize_line(raw, &p(), false);
        assert_eq!(b.events.len(), 0, "ai-title should not create a timeline event");
        assert_eq!(
            b.session.unwrap().title.as_deref(),
            Some("Refactor the ingest loop")
        );
    }

    #[test]
    fn control_lines_become_meta_with_labels() {
        let cases = [
            (r#"{"type":"mode","mode":"default","sessionId":"s1"}"#, "mode: default"),
            (
                r#"{"type":"permission-mode","permissionMode":"plan","sessionId":"s1"}"#,
                "permission: plan",
            ),
            (
                r#"{"type":"queue-operation","operation":"enqueue","sessionId":"s1"}"#,
                "queued: enqueue",
            ),
            // Attachment with no inner type still yields a usable label.
            (r#"{"type":"attachment","sessionId":"s1"}"#, "attachment"),
            (
                r#"{"type":"file-history-snapshot","sessionId":"s1"}"#,
                "file snapshot",
            ),
            (r#"{"type":"last-prompt","sessionId":"s1"}"#, "last prompt"),
            (
                r#"{"type":"bridge-session","bridgeSessionId":"abcdef1234","sessionId":"s1"}"#,
                "bridge session · abcdef12",
            ),
            // Attachment subtypes → informative, type-specific labels.
            (
                r#"{"type":"attachment","attachment":{"type":"hook_success","hookName":"format","hookEvent":"PreToolUse"},"sessionId":"s1"}"#,
                "attachment · hook_success · format",
            ),
            (
                r#"{"type":"attachment","attachment":{"type":"file","displayPath":"src/lib.rs"},"sessionId":"s1"}"#,
                "attachment · file · src/lib.rs",
            ),
            (
                r#"{"type":"attachment","attachment":{"type":"skill_listing","skillCount":27},"sessionId":"s1"}"#,
                "attachment · skill_listing · 27",
            ),
            (
                r#"{"type":"attachment","attachment":{"type":"deferred_tools_delta","addedNames":["a","b"],"removedNames":["c"]},"sessionId":"s1"}"#,
                "attachment · deferred_tools_delta · +2/-1",
            ),
            (
                r#"{"type":"attachment","attachment":{"type":"date_change","newDate":"2026-08-12"},"sessionId":"s1"}"#,
                "attachment · date_change · 2026-08-12",
            ),
            // Unknown attachment subtype → still shows the type, no detail.
            (
                r#"{"type":"attachment","attachment":{"type":"task_reminder"},"sessionId":"s1"}"#,
                "attachment · task_reminder",
            ),
        ];
        for (raw, expected_text) in cases {
            let b = normalize_line(raw, &p(), false);
            assert_eq!(b.events.len(), 1, "for {raw}");
            assert_eq!(b.events[0].kind, EventKind::Meta, "for {raw}");
            assert_eq!(b.events[0].text.as_deref(), Some(expected_text), "for {raw}");
        }
    }

    #[test]
    fn pr_link_becomes_clickable_system_link() {
        let raw = r#"{"type":"pr-link","sessionId":"s1","prNumber":42,"prUrl":"https://ex.test/org/repo/pull/42","prRepository":"org/repo","timestamp":"2026-08-08T00:00:00Z"}"#;
        let b = normalize_line(raw, &p(), false);
        assert_eq!(b.events.len(), 1);
        assert_eq!(b.events[0].kind, EventKind::System);
        let text = b.events[0].text.as_deref().unwrap();
        // A GitHub URL → "PR", rendered as a markdown link to the URL.
        assert_eq!(text, "[PR #42 · org/repo](https://ex.test/org/repo/pull/42)");
    }

    #[test]
    fn pr_link_detects_gitlab_merge_request() {
        let raw = r#"{"type":"pr-link","sessionId":"s1","prNumber":5,"prUrl":"https://ex.test/org/repo/-/merge_requests/5","prRepository":"org/repo","timestamp":"2026-08-08T00:00:00Z"}"#;
        let b = normalize_line(raw, &p(), false);
        let text = b.events[0].text.as_deref().unwrap();
        assert!(text.starts_with("[MR #5"), "text was {text}");
    }

    #[test]
    fn malformed_line_becomes_unknown_not_error() {
        let raw = "{not valid json";
        let b = normalize_line(raw, &p(), false);
        assert_eq!(b.events.len(), 1);
        assert_eq!(b.events[0].kind, EventKind::Unknown);
        assert_eq!(b.events[0].raw_json, raw);
        // session id falls back to the file stem
        assert_eq!(b.events[0].session_id, "cc:s1");
    }

    #[test]
    fn truly_unknown_line_type_stays_unknown() {
        // A type we don't recognize at all still falls back to Unknown.
        let raw = r#"{"type":"some-future-type","sessionId":"s1"}"#;
        let b = normalize_line(raw, &p(), false);
        assert_eq!(b.events.len(), 1);
        assert_eq!(b.events[0].kind, EventKind::Unknown);
    }

    #[test]
    fn summary_and_system_lines() {
        let sum = r#"{"type":"summary","summary":"did stuff","uuid":"x1"}"#;
        assert_eq!(
            normalize_line(sum, &p(), false).events[0].kind,
            EventKind::Summary
        );
        let sys = r#"{"type":"system","subtype":"hook","uuid":"x2"}"#;
        assert_eq!(
            normalize_line(sys, &p(), false).events[0].kind,
            EventKind::System
        );
    }

    #[test]
    fn sidechain_uses_file_identity_not_parent_session_id() {
        // Sidechain lines often carry the PARENT's sessionId; the subagent must
        // become its own session keyed by the file, never merge into the parent.
        let raw = r#"{"type":"user","sessionId":"parent-uuid","uuid":"u1","message":{"role":"user","content":"hi"}}"#;
        let sub_path = PathBuf::from("/home/u/.claude/projects/proj/subagents/agent-xyz.jsonl");
        let b = normalize_line(raw, &sub_path, true);
        let s = b.session.unwrap();
        assert_eq!(s.id, "cc:agent-xyz", "keyed by file stem, not parent sessionId");
        assert!(s.is_subagent);
        assert_eq!(
            s.parent_session_id.as_deref(),
            Some("cc:parent-uuid"),
            "real parent link from the sessionId field"
        );
        assert_eq!(b.events[0].session_id, "cc:agent-xyz");
    }

    /// End-to-end live-apply: start the real watch loop, then add a directory
    /// through `set_settings` and assert its sessions land WITHOUT a restart.
    ///
    /// Ignored by default because `run()` also picks up this machine's real
    /// `~/.claude` (the default root is implicit) and never returns — the thread
    /// is left to die with the test process.
    /// Run: `cargo test -- --ignored live_apply --nocapture`.
    #[test]
    #[ignore]
    fn adding_a_directory_is_picked_up_without_a_restart() {
        let (base, homes) = temp_homes(&[".claude-liveadd"], 2);
        let store = Store::open_in_memory().unwrap();
        let watcher_store = store.clone();
        std::thread::spawn(move || {
            let _ = run(watcher_store);
        });
        // Let the initial backfill + watcher setup settle.
        std::thread::sleep(Duration::from_millis(1500));
        let before = store
            .list_sessions(None)
            .unwrap()
            .iter()
            .filter(|s| s.account.as_deref() == Some("liveadd"))
            .count();
        assert_eq!(before, 0, "the new directory isn't watched yet");

        store
            .set_settings(crate::store::Settings {
                claude_dirs: vec![homes[0].to_string_lossy().to_string()],
                ..Default::default()
            })
            .unwrap();

        // The loop polls settings_gen once per FLUSH_INTERVAL (200ms).
        let mut found = 0;
        for _ in 0..40 {
            std::thread::sleep(Duration::from_millis(100));
            found = store
                .list_sessions(None)
                .unwrap()
                .iter()
                .filter(|s| s.account.as_deref() == Some("liveadd"))
                .count();
            if found == 2 {
                break;
            }
        }
        assert_eq!(found, 2, "both transcripts ingested after the settings change");

        // And a transcript written afterwards is tailed live by the new watcher.
        let proj = homes[0].join("projects").join("proj");
        std::fs::write(
            proj.join("live-1.jsonl"),
            "{\"type\":\"user\",\"sessionId\":\"live-1\",\"uuid\":\"u-live-1\",\"timestamp\":\"2026-08-08T00:00:00Z\",\"message\":{\"role\":\"user\",\"content\":\"hi\"}}\n",
        )
        .unwrap();
        let mut live = false;
        for _ in 0..40 {
            std::thread::sleep(Duration::from_millis(100));
            live = store
                .list_sessions(None)
                .unwrap()
                .iter()
                .any(|s| s.id == "cc:live-1");
            if live {
                break;
            }
        }
        assert!(live, "a file created after the add is tailed live");
        let _ = std::fs::remove_dir_all(&base);
    }

    /// Real-data smoke test: backfill the actual ~/.claude/projects into a temp
    /// DB (read-only against agent data). Ignored by default — depends on the
    /// machine having transcripts. Run: `cargo test -- --ignored real_backfill`.
    #[test]
    #[ignore]
    fn real_backfill_ingests_without_panic() {
        let store = Store::open_in_memory().unwrap();
        // Optionally exercise the multi-account path against real directories:
        //   ERIDIAN_TEST_CLAUDE_DIRS="~/.claude-a,~/.claude-b" \
        //     cargo test -- --ignored real_backfill --nocapture
        // Kept as an env var so no machine-specific directory name lives in the
        // repo. Unset → the default ~/.claude root only, as before.
        if let Ok(extra) = std::env::var("ERIDIAN_TEST_CLAUDE_DIRS") {
            let dirs: Vec<String> = extra
                .split(',')
                .map(str::trim)
                .filter(|d| !d.is_empty())
                .map(str::to_string)
                .collect();
            store
                .set_settings(crate::store::Settings {
                    claude_dirs: dirs,
                    ..Default::default()
                })
                .unwrap();
        }
        for root in claude_projects_dirs(&store) {
            eprintln!("root: {}", crate::paths::display_home(&root));
        }
        let n = backfill(&store, false).unwrap();
        let sessions = store.list_sessions(None).unwrap();
        let status = store.ingest_status().unwrap();
        eprintln!(
            "backfilled {n} files → {} sessions, {} cc events",
            sessions.len(),
            status.claude_code_events
        );
        assert!(n > 0, "expected at least one transcript file");
        assert!(!sessions.is_empty(), "expected at least one session");
        // NOT asserted: an event count. A machine whose ~/.claude/projects only
        // holds `ai-title` lines (normal once sessions moved to a per-account
        // CLAUDE_CONFIG_DIR) legitimately yields sessions and zero events. Point
        // ERIDIAN_TEST_CLAUDE_DIRS at a real account to exercise event ingest;
        // the idempotency assertion below is the one that holds either way.
        // Per-account rollup (counts only — never session titles or paths).
        let mut by_account: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        for s in &sessions {
            *by_account
                .entry(s.account.clone().unwrap_or_else(|| "<default>".into()))
                .or_default() += 1;
        }
        eprintln!("sessions per account: {by_account:?}");
        // Idempotency: a second backfill (simulating restart) adds nothing.
        backfill(&store, false).unwrap();
        let after = store.ingest_status().unwrap();
        assert_eq!(
            status.claude_code_events, after.claude_code_events,
            "restart backfill must not duplicate events"
        );
    }

    // ── fixture round-trip: normalize → store → query (PLAN.md M1) ────────────

    #[test]
    fn fixture_session_round_trips_through_store() {
        let fixture = include_str!("../../fixtures/claude_code_session.jsonl");
        let path = PathBuf::from("/tmp/demo/fix-1.jsonl");
        let mut batches = Vec::new();
        for line in fixture.lines() {
            if line.trim().is_empty() {
                continue;
            }
            batches.push(normalize_line(line, &path, false));
        }
        let store = Store::open_in_memory().unwrap();
        // A malformed line in the middle must not abort ingest of the rest.
        let inserted = store
            .commit_batches("/tmp/demo/fix-1.jsonl", 999, batches)
            .unwrap();
        assert!(inserted.len() >= 8, "expected all events, got {}", inserted.len());

        let sessions = store.list_sessions(None).unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, "cc:fix-1");
        // title backfilled from the first user prompt
        assert_eq!(
            sessions[0].title.as_deref(),
            Some("summarize the partition strategy")
        );
        assert_eq!(sessions[0].model.as_deref(), Some("claude-opus-4-8"));

        // Kinds present across the session.
        let events = store.session_events("cc:fix-1", 500, None).unwrap();
        let kinds: std::collections::HashSet<&str> =
            events.iter().map(|e| e.kind.as_str()).collect();
        // 'mode' line → meta; the malformed line → unknown.
        for expected in [
            "user", "assistant", "thinking", "tool_call", "tool_result", "summary",
            "system", "meta", "unknown",
        ] {
            assert!(kinds.contains(expected), "missing kind {expected}");
        }
    }

    #[test]
    fn usage_sums_input_and_all_cache_tokens() {
        // The whole prompt actually sent = input + cache_read + cache_creation.
        let v: Value = serde_json::from_str(
            r#"{"message":{"usage":{"input_tokens":5,"cache_read_input_tokens":300,"cache_creation_input_tokens":40,"output_tokens":12}}}"#,
        )
        .unwrap();
        assert_eq!(usage(&v), (Some(345), Some(12)));
    }

    #[test]
    fn usage_partial_and_missing_fields() {
        // Only cache_read present → still summed (input/creation default 0).
        let v: Value = serde_json::from_str(
            r#"{"message":{"usage":{"cache_read_input_tokens":100}}}"#,
        )
        .unwrap();
        assert_eq!(usage(&v), (Some(100), None));

        // No usage object at all → both None (not Some(0)).
        let empty: Value = serde_json::from_str(r#"{"message":{}}"#).unwrap();
        assert_eq!(usage(&empty), (None, None));
    }
}
