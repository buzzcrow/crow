// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Recursive-read infrastructure for two-tree GET handlers.
//!
//! Key work: `RecursiveDepth` parsing for the `?recursive=` query
//! parameter, a generic `Expandable` walk that any logical or physical
//! resource can implement, and a `Truncation` hint emitted when the
//! requested depth exceeds the capped maximum.
//!
//! The axum extractor wrapping `RecursiveDepth` lives in `crowdb-web`
//! because the `shared` crate stays axum-free.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Backend cap for `?recursive=all`. The design fixes this at 8 so even
/// the deepest tree (`rack → node → server → store → group → local
/// replica → remote`) fits comfortably without blowing up the response.
pub const MAX_DEPTH: u8 = 8;

/// Parsed `?recursive=` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RecursiveDepth {
    /// `recursive` absent or `recursive=0`. The handler returns only
    /// the addressed resource; child collections appear as counts /
    /// ids only.
    #[default]
    None,
    /// `recursive=N`, `1 <= N <= MAX_DEPTH`. The handler inlines N
    /// child hops.
    Levels(u8),
    /// `recursive=all`. Equivalent to `Levels(MAX_DEPTH)` semantically;
    /// the variant is preserved so the response can report it back.
    All,
}

impl RecursiveDepth {
    /// Effective integer depth used by the walk.
    #[must_use]
    pub fn effective(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Levels(n) => n,
            Self::All => MAX_DEPTH,
        }
    }

    /// Parse the raw `?recursive=` value. The empty string and `"0"`
    /// both yield `None`; `"all"` (any case) yields `All`; integers in
    /// `1..=MAX_DEPTH` yield `Levels(n)`. Everything else is a parse
    /// error so handlers can return `400 Validation`.
    ///
    /// # Errors
    /// Returns [`ParseError`] for out-of-range or malformed values.
    pub fn parse(raw: &str) -> Result<Self, ParseError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(Self::None);
        }
        if trimmed.eq_ignore_ascii_case("all") {
            return Ok(Self::All);
        }
        let n: u32 = trimmed
            .parse()
            .map_err(|_| ParseError::Malformed(raw.to_string()))?;
        if n == 0 {
            return Ok(Self::None);
        }
        if n > u32::from(MAX_DEPTH) {
            return Err(ParseError::OutOfRange {
                value: n,
                max: MAX_DEPTH,
            });
        }
        Ok(Self::Levels(u8::try_from(n).unwrap_or(MAX_DEPTH)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    Malformed(String),
    OutOfRange { value: u32, max: u8 },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(s) => write!(f, "recursive=\"{s}\" is not an integer or \"all\""),
            Self::OutOfRange { value, max } => write!(f, "recursive={value} exceeds maximum {max}"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Identifier of where the depth cap clipped the expansion. Emitted in
/// the response body as `truncated_at: [...]` so the caller can issue
/// targeted follow-up reads.
///
/// Each entry is the parent-chain path of the resource whose children
/// were not expanded — e.g. `["rack:r1", "node:n2"]` means the children
/// of `n2` inside `r1` were dropped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Truncation {
    pub paths: Vec<Vec<String>>,
}

impl Truncation {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    pub fn record(&mut self, path: Vec<String>) {
        self.paths.push(path);
    }
}

/// Trait every two-tree resource implements so handlers can walk
/// children uniformly. The walk is depth-first; `path` accumulates the
/// parent chain for `Truncation` reporting.
///
/// Implementations are pure-data: they pull children out of an
/// already-snapshotted view (typically the monitor cache). They must
/// not block or issue RPCs.
pub trait Expandable {
    /// `kind:id` segment representing this resource in the parent chain
    /// (e.g. `"rack:r1"`, `"node:n2"`, `"store:7"`).
    fn path_segment(&self) -> String;

    /// Walk children one level. Implementations call `visit` for each
    /// child; the framework drives the recursion. Returning early via
    /// `visit` short-circuiting is not supported — implementations must
    /// pass every child.
    fn walk_children(&self, visit: &mut dyn FnMut(&dyn Expandable));
}

/// Drive a depth-bounded walk of `root` and report what was truncated.
/// The `enter` callback fires once per visited resource (root + every
/// expanded child) so handlers can collect the serialized payload.
pub fn walk<F: FnMut(&dyn Expandable, &[String])>(
    root: &dyn Expandable,
    depth: RecursiveDepth,
    mut enter: F,
) -> Truncation {
    let mut trunc = Truncation::default();
    let mut path: Vec<String> = Vec::new();
    walk_inner(root, depth.effective(), &mut path, &mut enter, &mut trunc);
    trunc
}

fn walk_inner<F: FnMut(&dyn Expandable, &[String])>(
    node: &dyn Expandable,
    remaining: u8,
    path: &mut Vec<String>,
    enter: &mut F,
    trunc: &mut Truncation,
) {
    path.push(node.path_segment());
    enter(node, path);
    if remaining == 0 {
        // Probe: does the node actually have children? If so, the
        // walk was clipped here.
        let mut has_child = false;
        node.walk_children(&mut |_| {
            has_child = true;
        });
        if has_child {
            trunc.record(path.clone());
        }
    } else {
        node.walk_children(&mut |child| {
            walk_inner(child, remaining - 1, path, enter, trunc);
        });
    }
    path.pop();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_absent_or_zero_yields_none() {
        assert_eq!(RecursiveDepth::parse("").unwrap(), RecursiveDepth::None);
        assert_eq!(RecursiveDepth::parse("0").unwrap(), RecursiveDepth::None);
    }

    #[test]
    fn parse_levels_1_through_max() {
        for n in 1..=MAX_DEPTH {
            let raw = n.to_string();
            assert_eq!(RecursiveDepth::parse(&raw).unwrap(), RecursiveDepth::Levels(n));
        }
    }

    #[test]
    fn parse_all_case_insensitive() {
        assert_eq!(RecursiveDepth::parse("all").unwrap(), RecursiveDepth::All);
        assert_eq!(RecursiveDepth::parse("ALL").unwrap(), RecursiveDepth::All);
        assert_eq!(RecursiveDepth::parse("All").unwrap(), RecursiveDepth::All);
    }

    #[test]
    fn parse_out_of_range_errors() {
        let e = RecursiveDepth::parse("99").unwrap_err();
        assert!(matches!(e, ParseError::OutOfRange { .. }));
    }

    #[test]
    fn parse_malformed_errors() {
        let e = RecursiveDepth::parse("two").unwrap_err();
        assert!(matches!(e, ParseError::Malformed(_)));
    }

    #[test]
    fn effective_depth_for_all_is_max() {
        assert_eq!(RecursiveDepth::All.effective(), MAX_DEPTH);
    }

    // ── walk / Expandable ───────────────────────────────────────────

    struct TestNode {
        kind: &'static str,
        id: &'static str,
        children: Vec<TestNode>,
    }
    impl Expandable for TestNode {
        fn path_segment(&self) -> String {
            format!("{}:{}", self.kind, self.id)
        }
        fn walk_children(&self, visit: &mut dyn FnMut(&dyn Expandable)) {
            for c in &self.children {
                visit(c as &dyn Expandable);
            }
        }
    }

    fn sample() -> TestNode {
        TestNode {
            kind: "rack",
            id: "r1",
            children: vec![TestNode {
                kind: "node",
                id: "n1",
                children: vec![TestNode {
                    kind: "store",
                    id: "7",
                    children: vec![TestNode {
                        kind: "group",
                        id: "9",
                        children: vec![],
                    }],
                }],
            }],
        }
    }

    #[test]
    fn walk_depth_0_visits_only_root_and_truncates() {
        let root = sample();
        let mut visited: Vec<String> = Vec::new();
        let trunc = walk(&root, RecursiveDepth::None, |n, _p| {
            visited.push(n.path_segment());
        });
        assert_eq!(visited, vec!["rack:r1"]);
        assert_eq!(trunc.paths, vec![vec!["rack:r1".to_string()]]);
    }

    #[test]
    fn walk_depth_1_expands_immediate_children() {
        let root = sample();
        let mut visited: Vec<String> = Vec::new();
        let trunc = walk(&root, RecursiveDepth::Levels(1), |n, _p| {
            visited.push(n.path_segment());
        });
        assert_eq!(visited, vec!["rack:r1", "node:n1"]);
        // node:n1 has children (store:7) but we stopped at depth 1,
        // so the truncation hint points at the chain through n1.
        assert_eq!(
            trunc.paths,
            vec![vec!["rack:r1".to_string(), "node:n1".to_string()]]
        );
    }

    #[test]
    fn walk_depth_2_expands_two_levels() {
        let root = sample();
        let mut visited: Vec<String> = Vec::new();
        let trunc = walk(&root, RecursiveDepth::Levels(2), |n, _p| {
            visited.push(n.path_segment());
        });
        assert_eq!(visited, vec!["rack:r1", "node:n1", "store:7"]);
        assert_eq!(
            trunc.paths,
            vec![vec![
                "rack:r1".to_string(),
                "node:n1".to_string(),
                "store:7".to_string()
            ]]
        );
    }

    #[test]
    fn walk_all_expands_everything_without_truncation() {
        let root = sample();
        let mut visited: Vec<String> = Vec::new();
        let trunc = walk(&root, RecursiveDepth::All, |n, _p| {
            visited.push(n.path_segment());
        });
        assert_eq!(visited, vec!["rack:r1", "node:n1", "store:7", "group:9"]);
        assert!(trunc.is_empty());
    }
}
