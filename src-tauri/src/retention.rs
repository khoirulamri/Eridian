//! Retention policy: which sessions to drop, as pure functions.
//!
//! Split out from [`crate::store`] so the policy can be tested exhaustively
//! without a database — retention deletes are irreversible, so the decision of
//! *what* to delete must never depend on I/O timing or on a live metric that
//! might not move when we expect it to.
//!
//! Two independent controls, mirroring Settings:
//!   * a per-(agent, account) session cap — no account can starve another
//!   * a global size budget — the archive stays bounded on disk
//!
//! The account comes from [`crate::paths::account_label`], derived from
//! `sessions.source_ref`, so no schema column (and no re-ingest) is needed.

use std::collections::{HashMap, HashSet};

/// One session as retention sees it.
#[derive(Clone, Debug)]
pub struct Candidate {
    pub id: String,
    pub agent: String,
    /// `None` for the default `~/.claude` root and for every OpenCode session.
    pub account: Option<String>,
    pub updated_at: Option<String>,
}

/// Bucket key: sessions compete for a slot only within their own key.
type Bucket = (String, Option<String>);

fn bucket_of(c: &Candidate) -> Bucket {
    (c.agent.clone(), c.account.clone())
}

/// Group and sort each bucket newest-first. `updated_at` is ISO-8601 UTC, so a
/// lexicographic sort is chronological; `None` sorts oldest. Ties break on id so
/// the result is deterministic (important: these decide deletions).
fn buckets_newest_first(rows: &[Candidate]) -> HashMap<Bucket, Vec<&Candidate>> {
    let mut out: HashMap<Bucket, Vec<&Candidate>> = HashMap::new();
    for c in rows {
        out.entry(bucket_of(c)).or_default().push(c);
    }
    for v in out.values_mut() {
        v.sort_by(|a, b| {
            b.updated_at
                .as_deref()
                .unwrap_or("")
                .cmp(a.updated_at.as_deref().unwrap_or(""))
                .then_with(|| a.id.cmp(&b.id))
        });
    }
    out
}

/// Sessions to drop so that each (agent, account) bucket keeps at most `max`.
///
/// `max <= 0` means "no cap" (mirrors the 0-means-off idiom used by
/// `Store::backfill_file_limit`).
pub fn over_account_cap(rows: &[Candidate], max: i64) -> Vec<String> {
    if max <= 0 {
        return Vec::new();
    }
    let max = max as usize;
    let mut out: Vec<String> = buckets_newest_first(rows)
        .into_values()
        .flat_map(|v| {
            v.into_iter()
                .skip(max)
                .map(|c| c.id.clone())
                .collect::<Vec<_>>()
        })
        .collect();
    out.sort();
    out
}

/// Oldest-first eviction order for the size budget.
///
/// Never yields a bucket's most recent session: a size budget must not be able to
/// erase an account outright, however small the number the user typed.
pub fn eviction_order(rows: &[Candidate]) -> Vec<String> {
    let protected: HashSet<&str> = buckets_newest_first(rows)
        .into_values()
        .filter_map(|v| v.first().map(|c| c.id.as_str()))
        .collect();

    let mut evictable: Vec<&Candidate> = rows
        .iter()
        .filter(|c| !protected.contains(c.id.as_str()))
        .collect();
    // Oldest first, so the budget takes the least valuable sessions.
    evictable.sort_by(|a, b| {
        a.updated_at
            .as_deref()
            .unwrap_or("")
            .cmp(b.updated_at.as_deref().unwrap_or(""))
            .then_with(|| a.id.cmp(&b.id))
    });
    evictable.into_iter().map(|c| c.id.clone()).collect()
}

/// Walk `order` subtracting each session's bytes until the archive fits
/// `budget_bytes`, and return exactly that set.
///
/// Deliberately computed up front rather than by deleting and re-measuring: a
/// loop driven by a live size metric that doesn't drop as expected would run the
/// eviction order to exhaustion and gut the archive. All values are raw
/// `raw_json` bytes; the caller converts the user's budget into the same space.
///
/// `max_evictions` caps a single pass — a bug in the byte accounting then costs
/// one sweep rather than the archive.
pub fn select_for_budget(
    order: &[String],
    bytes_by_id: &HashMap<String, i64>,
    total_bytes: i64,
    budget_bytes: i64,
    max_evictions: usize,
) -> Vec<String> {
    if budget_bytes <= 0 || total_bytes <= budget_bytes {
        return Vec::new();
    }
    let mut remaining = total_bytes;
    let mut out = Vec::new();
    for id in order {
        if remaining <= budget_bytes || out.len() >= max_evictions {
            break;
        }
        remaining -= bytes_by_id.get(id).copied().unwrap_or(0);
        out.push(id.clone());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(id: &str, account: Option<&str>, updated: Option<&str>) -> Candidate {
        Candidate {
            id: id.into(),
            agent: "claude-code".into(),
            account: account.map(str::to_string),
            updated_at: updated.map(str::to_string),
        }
    }

    /// Mirrors the real distribution that motivated this change: one account with
    /// almost everything, another with a single session.
    fn skewed() -> Vec<Candidate> {
        let mut rows: Vec<Candidate> = (0..10)
            .map(|i| {
                c(
                    &format!("cc:busy{i}"),
                    Some("busy"),
                    Some(&format!("2026-08-{:02}T00:00:00Z", i + 1)),
                )
            })
            .collect();
        rows.push(c("cc:quiet0", Some("quiet"), Some("2026-01-01T00:00:00Z")));
        rows
    }

    #[test]
    fn cap_applies_per_bucket_so_a_busy_account_cannot_starve_a_quiet_one() {
        let rows = skewed();
        let dropped = over_account_cap(&rows, 3);
        // The quiet account's single (and much older) session survives, which a
        // global cap ordered by recency would have evicted first.
        assert!(!dropped.contains(&"cc:quiet0".to_string()));
        // Busy keeps its 3 newest → 7 dropped.
        assert_eq!(dropped.len(), 7);
        assert!(dropped.contains(&"cc:busy0".to_string()));
        assert!(!dropped.contains(&"cc:busy9".to_string()));
    }

    #[test]
    fn cap_of_zero_or_less_means_no_cap() {
        let rows = skewed();
        assert!(over_account_cap(&rows, 0).is_empty());
        assert!(over_account_cap(&rows, -1).is_empty());
    }

    #[test]
    fn default_root_and_opencode_are_separate_buckets_from_labeled_accounts() {
        let rows = vec![
            c("cc:default0", None, Some("2026-08-01T00:00:00Z")),
            c("cc:default1", None, Some("2026-08-02T00:00:00Z")),
            c("cc:work0", Some("work"), Some("2026-08-01T00:00:00Z")),
            Candidate {
                id: "oc:1".into(),
                agent: "opencode".into(),
                account: None,
                updated_at: Some("2026-08-01T00:00:00Z".into()),
            },
        ];
        let dropped = over_account_cap(&rows, 1);
        // Only the default cc bucket is over the cap; opencode shares no bucket
        // with the unlabeled claude-code sessions despite both having account None.
        assert_eq!(dropped, vec!["cc:default0".to_string()]);
    }

    #[test]
    fn missing_timestamps_sort_oldest_and_ties_are_deterministic() {
        let rows = vec![
            c("cc:b", Some("a"), None),
            c("cc:a", Some("a"), None),
            c("cc:new", Some("a"), Some("2026-08-09T00:00:00Z")),
        ];
        assert_eq!(over_account_cap(&rows, 1), vec!["cc:a", "cc:b"]);
        // Same input, same answer — no HashMap iteration order leaking through.
        for _ in 0..5 {
            assert_eq!(over_account_cap(&rows, 1), vec!["cc:a", "cc:b"]);
        }
    }

    #[test]
    fn eviction_order_is_oldest_first_and_protects_each_buckets_newest() {
        let rows = skewed();
        let order = eviction_order(&rows);
        // One protected session per bucket (busy9, quiet0) → 11 - 2 = 9.
        assert_eq!(order.len(), 9);
        assert!(!order.contains(&"cc:busy9".to_string()));
        assert!(
            !order.contains(&"cc:quiet0".to_string()),
            "a size budget must never erase an account entirely"
        );
        assert_eq!(order.first().unwrap(), "cc:busy0", "oldest goes first");
    }

    #[test]
    fn a_single_session_account_is_never_evictable() {
        let rows = vec![c("cc:only", Some("solo"), Some("2020-01-01T00:00:00Z"))];
        assert!(eviction_order(&rows).is_empty());
    }

    #[test]
    fn budget_stops_as_soon_as_the_archive_fits() {
        let order: Vec<String> = ["a", "b", "c", "d"].iter().map(|s| s.to_string()).collect();
        let bytes: HashMap<String, i64> = order.iter().cloned().zip([100, 100, 100, 100]).collect();
        // 400 total, budget 250 → drop the two oldest (400-100-100 = 200 ≤ 250).
        let picked = select_for_budget(&order, &bytes, 400, 250, 100);
        assert_eq!(picked, vec!["a", "b"]);
    }

    #[test]
    fn budget_is_a_no_op_when_already_under_or_unset() {
        let order = vec!["a".to_string()];
        let bytes: HashMap<String, i64> = [("a".to_string(), 10)].into_iter().collect();
        assert!(select_for_budget(&order, &bytes, 100, 100, 10).is_empty());
        assert!(select_for_budget(&order, &bytes, 100, 500, 10).is_empty());
        // 0 / negative budget = "no cap", not "delete everything".
        assert!(select_for_budget(&order, &bytes, 100, 0, 10).is_empty());
        assert!(select_for_budget(&order, &bytes, 100, -5, 10).is_empty());
    }

    #[test]
    fn budget_honors_the_per_pass_ceiling() {
        let order: Vec<String> = (0..10).map(|i| format!("s{i}")).collect();
        let bytes: HashMap<String, i64> = order.iter().cloned().map(|id| (id, 10)).collect();
        // Would need all 10 to get under, but the pass may only take 3.
        let picked = select_for_budget(&order, &bytes, 100, 1, 3);
        assert_eq!(picked.len(), 3);
        assert_eq!(picked, vec!["s0", "s1", "s2"]);
    }

    #[test]
    fn budget_cannot_exceed_the_eviction_order_even_if_it_never_fits() {
        // An absurdly small budget must terminate at the protected floor, not
        // spin or over-delete.
        let order: Vec<String> = vec!["a".into(), "b".into()];
        let bytes: HashMap<String, i64> = order.iter().cloned().map(|id| (id, 1)).collect();
        let picked = select_for_budget(&order, &bytes, 1_000_000, 1, 999);
        assert_eq!(picked, order, "at most every evictable session, never more");
    }

    #[test]
    fn sessions_missing_from_the_byte_map_count_as_zero_not_a_panic() {
        let order = vec!["ghost".to_string(), "real".to_string()];
        let bytes: HashMap<String, i64> = [("real".to_string(), 100)].into_iter().collect();
        let picked = select_for_budget(&order, &bytes, 100, 50, 10);
        assert_eq!(picked, vec!["ghost", "real"]);
    }
}
