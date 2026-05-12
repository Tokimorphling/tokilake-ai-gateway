//! Channel index — in-memory routing table for model → channel resolution.
//!
//! Mirrors Go's `ChannelsChooser` / `ChannelGroup` in `model/balancer.go`.
//! Supports:
//!   - Group-based routing (primary → backup group fallback)
//!   - Exact model match → wildcard/prefix fallback
//!   - Priority tiers (highest first)
//!   - Weighted random selection within same priority
//!   - Cooldown management for rate-limited channels
//!   - Per-channel model name mapping

use dashmap::DashMap;
use parking_lot::RwLock;
use rand::Rng;
use std::{collections::HashMap, time::Instant};

// ---------------------------------------------------------------------------
// Channel entry — lightweight, clone-friendly
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ChannelEntry {
    pub id:            i64,
    pub name:          String,
    pub provider:      String,
    pub models:        String,
    pub base_url:      Option<String>,
    pub api_key:       Option<String>,
    pub status:        i32,
    pub weight:        u32,
    pub group:         String,
    pub priority:      i64,
    /// JSON-encoded model mapping: `{"old_name": "new_name"}`.
    pub model_mapping: Option<String>,
}

impl ChannelEntry {
    /// Apply per-channel model name mapping.
    /// Returns `(mapped_name, use_original_for_billing)`.
    pub fn map_model(&self, model: &str) -> (String, bool) {
        let Some(ref json) = self.model_mapping else {
            return (model.into(), false);
        };
        let Ok(map) = serde_json::from_str::<HashMap<String, String>>(json) else {
            return (model.into(), false);
        };
        match map.get(model) {
            Some(m) if m.starts_with('+') => (m[1..].into(), true),
            Some(m) => (m.clone(), false),
            None => (model.into(), false),
        }
    }
}

// ---------------------------------------------------------------------------
// ChannelIndex — the central routing table
// ---------------------------------------------------------------------------

struct Cooldown {
    until: Instant,
}

/// In-memory channel index, lock-free for concurrent reads.
///
/// Structure mirrors Go's `ChannelsChooser`:
/// ```text
/// Rule: HashMap<group, HashMap<model, Vec<Vec<ChannelId>>>>
///       group → model → priority_tiers (sorted desc) → channel_ids
/// ```
pub struct ChannelIndex {
    channels:       DashMap<i64, ChannelEntry>,
    rule:           RwLock<HashMap<String, HashMap<String, Vec<Vec<i64>>>>>,
    match_patterns: RwLock<HashMap<String, HashMap<String, Vec<i64>>>>,
    model_groups:   RwLock<HashMap<String, Vec<String>>>,
    cooldowns:      DashMap<String, Cooldown>,
}

impl ChannelIndex {
    pub fn new() -> Self {
        Self {
            channels:       DashMap::new(),
            rule:           RwLock::new(HashMap::new()),
            match_patterns: RwLock::new(HashMap::new()),
            model_groups:   RwLock::new(HashMap::new()),
            cooldowns:      DashMap::new(),
        }
    }

    // -- Mutations -----------------------------------------------------------

    pub fn upsert(&self, ch: ChannelEntry) {
        self.channels.insert(ch.id, ch);
        self.rebuild();
    }

    pub fn remove(&self, id: i64) {
        self.channels.remove(&id);
        self.rebuild();
    }

    pub fn set_cooldown(&self, channel_id: i64, model: &str, duration: std::time::Duration) {
        self.cooldowns
            .insert(format!("{channel_id}:{model}"), Cooldown {
                until: Instant::now() + duration,
            });
    }

    // -- Queries -------------------------------------------------------------

    pub fn select(&self, group: &str, model: &str) -> Option<ChannelEntry> {
        self.select_with_filter(group, model, &[])
    }

    pub fn select_with_filter(
        &self,
        group: &str,
        model: &str,
        skip_ids: &[i64],
    ) -> Option<ChannelEntry> {
        // 1. Exact match
        let tiers = {
            let rule = self.rule.read();
            rule.get(group).and_then(|m| m.get(model)).cloned()
        };
        if let Some(tiers) = tiers {
            if let Some(ch) = self.select_from_tiers(&tiers, model, skip_ids) {
                return Some(ch);
            }
        }

        // 2. Wildcard/prefix fallback
        let matched_ids = {
            let patterns = self.match_patterns.read();
            patterns.get(group).and_then(|m| {
                for (pattern, ids) in m {
                    let prefix = pattern.trim_end_matches('*');
                    if model.starts_with(prefix) {
                        return Some(ids.clone());
                    }
                }
                None
            })
        };
        if let Some(ids) = matched_ids {
            if let Some(ch) = self.select_from_tiers(&[ids], model, skip_ids) {
                return Some(ch);
            }
        }

        None
    }

    pub fn select_with_groups(
        &self,
        groups: &[&str],
        model: &str,
        skip_ids: &[i64],
    ) -> Option<ChannelEntry> {
        for &group in groups {
            if let Some(ch) = self.select_with_filter(group, model, skip_ids) {
                return Some(ch);
            }
        }
        None
    }

    pub fn models_for_group(&self, group: &str) -> Vec<String> {
        self.rule
            .read()
            .get(group)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    pub fn get(&self, id: i64) -> Option<ChannelEntry> {
        self.channels.get(&id).map(|r| r.value().clone())
    }

    // -- Internal ------------------------------------------------------------

    fn rebuild(&self) {
        let mut rule: HashMap<_, HashMap<_, Vec<Vec<_>>>> = HashMap::new();
        let mut match_patterns: HashMap<_, HashMap<_, Vec<_>>> = HashMap::new();
        let mut model_groups: HashMap<_, Vec<_>> = HashMap::new();

        for entry in self.channels.iter() {
            let ch = entry.value();
            if ch.status != 1 {
                continue;
            }

            for group in ch.group.split(',').map(str::trim) {
                for model in ch.models.split(',').map(str::trim) {
                    model_groups
                        .entry(model.into())
                        .or_default()
                        .push(group.into());

                    if model.ends_with('*') {
                        match_patterns
                            .entry(group.into())
                            .or_default()
                            .entry(model.into())
                            .or_default()
                            .push(ch.id);
                    } else {
                        rule.entry(group.into())
                            .or_default()
                            .entry(model.into())
                            .or_default()
                            .push(vec![ch.id]);
                    }
                }
            }
        }

        // Rebuild priority tiers: sort by priority desc, group into tiers
        for group_map in rule.values_mut() {
            for tiers in group_map.values_mut() {
                let mut entries: Vec<_> = tiers
                    .iter()
                    .flatten()
                    .filter_map(|&id| self.channels.get(&id).map(|ch| (ch.priority, id)))
                    .collect();
                entries.sort_by(|a, b| b.0.cmp(&a.0));

                let mut new_tiers = Vec::new();
                let mut current_prio = None;
                let mut current_tier = Vec::new();

                for (prio, id) in entries {
                    if current_prio != Some(prio) {
                        if !current_tier.is_empty() {
                            new_tiers.push(std::mem::take(&mut current_tier));
                        }
                        current_prio = Some(prio);
                    }
                    current_tier.push(id);
                }
                if !current_tier.is_empty() {
                    new_tiers.push(current_tier);
                }

                *tiers = new_tiers;
            }
        }

        *self.rule.write() = rule;
        *self.match_patterns.write() = match_patterns;
        *self.model_groups.write() = model_groups;
    }

    fn select_from_tiers(
        &self,
        tiers: &[Vec<i64>],
        model: &str,
        skip_ids: &[i64],
    ) -> Option<ChannelEntry> {
        for tier in tiers {
            let mut valid = Vec::new();
            let mut total_weight: u64 = 0;

            for &id in tier {
                if skip_ids.contains(&id) {
                    continue;
                }

                let cooldown_key = format!("{id}:{model}");
                if let Some(cd) = self.cooldowns.get(&cooldown_key) {
                    if Instant::now() < cd.until {
                        continue;
                    }
                    drop(cd);
                    self.cooldowns.remove(&cooldown_key);
                }

                if let Some(ch) = self.channels.get(&id) {
                    if ch.status != 1 {
                        continue;
                    }
                    let w = ch.weight.max(1) as u64;
                    valid.push((id, w));
                    total_weight += w;
                }
            }

            if valid.is_empty() {
                continue;
            }

            if valid.len() == 1 {
                return self.get(valid[0].0);
            }

            let pick = rand::rng().random_range(0..total_weight);
            let mut remaining = pick;
            for (id, weight) in &valid {
                remaining = remaining.saturating_sub(*weight);
                if remaining == 0 || *weight > remaining {
                    return self.get(*id);
                }
            }

            return self.get(valid[0].0);
        }

        None
    }
}

impl Default for ChannelIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ChannelIndex {
    fn clone(&self) -> Self {
        let new = Self::new();
        for entry in self.channels.iter() {
            new.channels.insert(*entry.key(), entry.value().clone());
        }
        *new.rule.write() = self.rule.read().clone();
        *new.match_patterns.write() = self.match_patterns.read().clone();
        *new.model_groups.write() = self.model_groups.read().clone();
        new
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_channel(
        id: i64,
        models: &str,
        group: &str,
        priority: i64,
        weight: u32,
    ) -> ChannelEntry {
        ChannelEntry {
            id,
            name: format!("ch-{id}"),
            provider: "openai".into(),
            models: models.into(),
            base_url: Some("https://api.openai.com".into()),
            api_key: Some("sk-test".into()),
            status: 1,
            weight,
            group: group.into(),
            priority,
            model_mapping: None,
        }
    }

    #[test]
    fn exact_match() {
        let idx = ChannelIndex::new();
        idx.upsert(make_channel(1, "gpt-4,gpt-3.5-turbo", "default", 0, 1));
        assert_eq!(idx.select("default", "gpt-4").unwrap().id, 1);
    }

    #[test]
    fn wildcard_match() {
        let idx = ChannelIndex::new();
        idx.upsert(make_channel(1, "gpt-4*", "default", 0, 1));
        assert_eq!(idx.select("default", "gpt-4o").unwrap().id, 1);
    }

    #[test]
    fn priority_ordering() {
        let idx = ChannelIndex::new();
        idx.upsert(make_channel(1, "gpt-4", "default", 0, 1));
        idx.upsert(make_channel(2, "gpt-4", "default", 10, 1));
        assert_eq!(idx.select("default", "gpt-4").unwrap().id, 2);
    }

    #[test]
    fn group_isolation() {
        let idx = ChannelIndex::new();
        idx.upsert(make_channel(1, "gpt-4", "default", 0, 1));
        idx.upsert(make_channel(2, "gpt-4", "vip", 0, 1));
        assert_eq!(idx.select("default", "gpt-4").unwrap().id, 1);
        assert_eq!(idx.select("vip", "gpt-4").unwrap().id, 2);
        assert!(idx.select("other", "gpt-4").is_none());
    }

    #[test]
    fn skip_filtered() {
        let idx = ChannelIndex::new();
        idx.upsert(make_channel(1, "gpt-4", "default", 0, 1));
        idx.upsert(make_channel(2, "gpt-4", "default", 0, 1));
        assert_eq!(
            idx.select_with_filter("default", "gpt-4", &[1]).unwrap().id,
            2
        );
    }

    #[test]
    fn select_with_groups_fallback() {
        let idx = ChannelIndex::new();
        idx.upsert(make_channel(1, "gpt-4", "vip", 0, 1));
        assert_eq!(
            idx.select_with_groups(&["default", "vip"], "gpt-4", &[])
                .unwrap()
                .id,
            1
        );
    }

    #[test]
    fn model_mapping() {
        let mut ch = make_channel(1, "gpt-4", "default", 0, 1);
        ch.model_mapping = Some(r#"{"gpt-4": "gpt-4-2024-08-06"}"#.into());
        let (mapped, billing) = ch.map_model("gpt-4");
        assert_eq!(mapped, "gpt-4-2024-08-06");
        assert!(!billing);
    }

    #[test]
    fn model_mapping_plus_prefix() {
        let mut ch = make_channel(1, "gpt-4", "default", 0, 1);
        ch.model_mapping = Some(r#"{"gpt-4": "+gpt-4-2024-08-06"}"#.into());
        let (mapped, billing) = ch.map_model("gpt-4");
        assert_eq!(mapped, "gpt-4-2024-08-06");
        assert!(billing);
    }
}
