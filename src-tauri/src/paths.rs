//! Small pure path helpers shared by the ingester, the store and the command
//! layer. Kept dependency-free and side-effect-free so they stay unit-testable.
//!
//! Guardrail note: `display_home` exists so the UI can show a watched directory
//! without leaking the machine's absolute home path (which contains the OS
//! username). Never send a raw absolute path to the frontend — send this.

use std::path::{Component, Path, PathBuf};

/// Expand a user-typed directory string into an absolute path.
///
/// Accepts `~`, `~/...` and absolute paths; trims surrounding whitespace and any
/// trailing separators. Relative paths are rejected (`None`) — a watch root must
/// be unambiguous, and the ingest thread has no meaningful working directory.
pub fn expand_home(s: &str) -> Option<PathBuf> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let expanded = if s == "~" {
        dirs::home_dir()?
    } else if let Some(rest) = s.strip_prefix("~/") {
        dirs::home_dir()?.join(rest)
    } else {
        PathBuf::from(s)
    };
    if !expanded.is_absolute() {
        return None;
    }
    // Drop trailing separators so "~/.claude-x/" and "~/.claude-x" dedupe.
    let trimmed = expanded.to_string_lossy().trim_end_matches('/').to_string();
    if trimmed.is_empty() {
        return Some(PathBuf::from("/"));
    }
    Some(PathBuf::from(trimmed))
}

/// Render an absolute path for display, collapsing the home prefix to `~`.
pub fn display_home(p: &Path) -> String {
    if let Some(home) = dirs::home_dir() {
        if let Ok(rest) = p.strip_prefix(&home) {
            if rest.as_os_str().is_empty() {
                return "~".to_string();
            }
            return format!("~/{}", rest.display());
        }
    }
    p.display().to_string()
}

/// Derive a short account label from a Claude Code transcript path.
///
/// The label is the name of the directory holding `projects/`, with the
/// `.claude` prefix stripped: `~/.claude-work/projects/…` → `work`. The plain
/// default root (`~/.claude`) yields `None` so the UI shows no chip for it.
///
/// Derived from the path rather than stored on the row, so the label stays
/// correct after a directory is removed from the watch list, and no schema
/// change (and therefore no full re-ingest) is needed.
pub fn account_label(source_ref: &str) -> Option<String> {
    let path = Path::new(source_ref);
    // Walk to the LAST `projects` segment and remember the one before it.
    let mut prev: Option<String> = None;
    let mut parent_of_projects: Option<String> = None;
    for c in path.components() {
        if let Component::Normal(os) = c {
            let name = os.to_string_lossy();
            if name == "projects" {
                parent_of_projects = prev.clone();
            }
            prev = Some(name.to_string());
        }
    }
    let dir = parent_of_projects?;
    let dir = dir.strip_prefix('.').unwrap_or(&dir);
    if dir == "claude" || dir.is_empty() {
        return None;
    }
    let label = dir
        .strip_prefix("claude-")
        .or_else(|| dir.strip_prefix("claude_"))
        .unwrap_or(dir);
    (!label.is_empty()).then(|| label.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_label_strips_the_claude_prefix() {
        assert_eq!(
            account_label("/h/.claude-work/projects/proj/s1.jsonl").as_deref(),
            Some("work")
        );
        assert_eq!(
            account_label("/h/.claude_work/projects/proj/s1.jsonl").as_deref(),
            Some("work")
        );
    }

    #[test]
    fn default_root_has_no_label() {
        assert_eq!(account_label("/h/.claude/projects/proj/s1.jsonl"), None);
    }

    #[test]
    fn subagent_paths_resolve_to_the_same_label() {
        assert_eq!(
            account_label("/h/.claude-alpha/projects/p/subagents/agent-x.jsonl").as_deref(),
            Some("alpha")
        );
    }

    #[test]
    fn non_claude_root_keeps_its_whole_name() {
        assert_eq!(
            account_label("/data/work-claude/projects/p/s.jsonl").as_deref(),
            Some("work-claude")
        );
    }

    #[test]
    fn a_project_named_projects_uses_the_outermost_root() {
        // The LAST `projects` segment wins, so a project directory that happens
        // to be called "projects" doesn't fool the label.
        assert_eq!(
            account_label("/h/.claude-alpha/projects/projects/s.jsonl").as_deref(),
            Some("projects"),
        );
    }

    #[test]
    fn paths_without_a_projects_segment_have_no_label() {
        // OpenCode stores an API session id in source_ref, not a path.
        assert_eq!(account_label("ses_abc123"), None);
        assert_eq!(account_label("/h/.claude-x/other/s.jsonl"), None);
    }

    #[test]
    fn expand_home_resolves_tilde_and_trims() {
        let home = dirs::home_dir().unwrap();
        assert_eq!(expand_home("~/.claude-x").unwrap(), home.join(".claude-x"));
        assert_eq!(
            expand_home("  ~/.claude-x/  ").unwrap(),
            home.join(".claude-x")
        );
        assert_eq!(expand_home("~").unwrap(), home);
        assert_eq!(expand_home("/abs/dir").unwrap(), PathBuf::from("/abs/dir"));
    }

    #[test]
    fn expand_home_rejects_empty_and_relative() {
        assert!(expand_home("").is_none());
        assert!(expand_home("   ").is_none());
        assert!(expand_home("relative/dir").is_none());
        assert!(expand_home("./dir").is_none());
    }

    #[test]
    fn display_home_collapses_the_home_prefix() {
        let home = dirs::home_dir().unwrap();
        assert_eq!(display_home(&home.join(".claude-x")), "~/.claude-x");
        assert_eq!(display_home(&home), "~");
        assert_eq!(display_home(Path::new("/opt/elsewhere")), "/opt/elsewhere");
    }

    #[test]
    fn expand_then_display_round_trips() {
        let p = expand_home("~/.claude-alpha").unwrap();
        assert_eq!(display_home(&p), "~/.claude-alpha");
    }
}
