use crate::error::NotificationError;
use parqtel_core::config::{parse_matcher_str, MatchOp};
use regex::Regex;
use std::collections::BTreeMap;

/// A compiled Alertmanager label matcher (`=`, `!=`, `=~`, `!~`).
#[derive(Debug, Clone)]
pub struct CompiledMatcher {
    pub label: String,
    pub op: MatchOp,
    pub value: String,
    regex: Option<Regex>,
}

impl CompiledMatcher {
    /// Parse and compile an Alertmanager matcher expression such as `severity = "critical"`
    /// or `service.name =~ "api-.*"`.
    pub fn compile(spec: &str) -> Result<Self, NotificationError> {
        let parsed = parse_matcher_str(spec).map_err(NotificationError::Config)?;
        let regex = match parsed.op {
            MatchOp::Regex | MatchOp::NotRegex => {
                let anchored = format!("^(?:{})$", parsed.value);
                let re = Regex::new(&anchored).map_err(|e| {
                    NotificationError::Config(format!(
                        "invalid regex '{}' in matcher '{spec}': {e}",
                        parsed.value
                    ))
                })?;
                Some(re)
            }
            MatchOp::Eq | MatchOp::Ne => None,
        };

        Ok(Self {
            label: parsed.label,
            op: parsed.op,
            value: parsed.value,
            regex,
        })
    }

    /// Evaluate this matcher against a label map using Alertmanager semantics.
    ///
    /// Missing labels are treated as `""` (empty string).
    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        let actual = labels.get(&self.label).map(String::as_str).unwrap_or("");
        match self.op {
            MatchOp::Eq => actual == self.value,
            MatchOp::Ne => actual != self.value,
            MatchOp::Regex => self.regex.as_ref().is_some_and(|re| re.is_match(actual)),
            MatchOp::NotRegex => self.regex.as_ref().is_some_and(|re| !re.is_match(actual)),
        }
    }
}

/// A conjunction of compiled label matchers. An empty `MatcherSet` matches everything.
#[derive(Debug, Clone, Default)]
pub struct MatcherSet(Vec<CompiledMatcher>);

impl MatcherSet {
    /// Compile all matcher expressions in `specs`.
    pub fn compile_all(specs: &[String]) -> Result<Self, NotificationError> {
        let mut matchers = Vec::with_capacity(specs.len());
        for spec in specs {
            matchers.push(CompiledMatcher::compile(spec)?);
        }
        Ok(Self(matchers))
    }

    /// Returns `true` if every matcher in the set matches `labels` (or if the set is empty).
    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        self.0.iter().all(|m| m.matches(labels))
    }

    /// Returns the compiled matchers as a slice.
    pub fn as_slice(&self) -> &[CompiledMatcher] {
        &self.0
    }

    /// Returns `true` if the set contains no matchers.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of matchers in the set.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn test_eq_matcher_present_and_missing() {
        let m = CompiledMatcher::compile(r#"severity = "critical""#).unwrap();
        assert_eq!(m.label, "severity");
        assert_eq!(m.op, MatchOp::Eq);
        assert_eq!(m.value, "critical");

        assert!(m.matches(&labels(&[("severity", "critical")])));
        assert!(!m.matches(&labels(&[("severity", "warning")])));
        assert!(!m.matches(&labels(&[])));
    }

    #[test]
    fn test_eq_empty_string_matches_missing_label() {
        let m = CompiledMatcher::compile(r#"env = """#).unwrap();
        assert!(m.matches(&labels(&[])));
        assert!(m.matches(&labels(&[("env", "")])));
        assert!(!m.matches(&labels(&[("env", "prod")])));
    }

    #[test]
    fn test_ne_matcher_present_and_missing() {
        let m = CompiledMatcher::compile("env != dev").unwrap();
        assert!(m.matches(&labels(&[("env", "prod")])));
        assert!(!m.matches(&labels(&[("env", "dev")])));
        // Missing label is treated as "", which != "dev".
        assert!(m.matches(&labels(&[])));

        let ne_empty = CompiledMatcher::compile(r#"env != """#).unwrap();
        assert!(!ne_empty.matches(&labels(&[])));
        assert!(ne_empty.matches(&labels(&[("env", "prod")])));
    }

    #[test]
    fn test_regex_matcher_is_fully_anchored() {
        let m = CompiledMatcher::compile("service.name =~ 'api-.*'").unwrap();
        assert!(m.matches(&labels(&[("service.name", "api-gateway")])));
        assert!(m.matches(&labels(&[("service.name", "api-")])));
        // Must NOT match substring when anchored with ^(?:...)$.
        assert!(!m.matches(&labels(&[("service.name", "my-api-gateway")])));
        // Missing label treated as "", does not match api-.*.
        assert!(!m.matches(&labels(&[])));
    }

    #[test]
    fn test_regex_matching_empty_string_on_missing_label() {
        let m = CompiledMatcher::compile("env =~ '.*'").unwrap();
        assert!(m.matches(&labels(&[])));

        let opt = CompiledMatcher::compile("env =~ '(prod)?'").unwrap();
        assert!(opt.matches(&labels(&[])));
        assert!(opt.matches(&labels(&[("env", "prod")])));
        assert!(!opt.matches(&labels(&[("env", "staging")])));
    }

    #[test]
    fn test_not_regex_matcher_present_and_missing() {
        let m = CompiledMatcher::compile("team !~ test-.*").unwrap();
        assert!(m.matches(&labels(&[("team", "platform")])));
        assert!(!m.matches(&labels(&[("team", "test-infra")])));
        // Partial match does not trigger anchored regex, so !~ returns true.
        assert!(m.matches(&labels(&[("team", "my-test-infra")])));
        // Missing label treated as "", does not match test-.*, so !~ returns true.
        assert!(m.matches(&labels(&[])));

        let nre_empty = CompiledMatcher::compile("team !~ '.*'").unwrap();
        assert!(!nre_empty.matches(&labels(&[])));
    }

    #[test]
    fn test_compile_errors() {
        assert!(CompiledMatcher::compile("").is_err());
        assert!(CompiledMatcher::compile("no_operator").is_err());
        assert!(CompiledMatcher::compile("=val").is_err());
        assert!(CompiledMatcher::compile("service =~ [invalid").is_err());
        assert!(CompiledMatcher::compile("service !~ (unclosed").is_err());
    }

    #[test]
    fn test_matcher_set_empty_and_conjunction() {
        let empty = MatcherSet::compile_all(&[]).unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        assert!(empty.as_slice().is_empty());
        assert!(empty.matches(&labels(&[])));
        assert!(empty.matches(&labels(&[("a", "b")])));

        let set = MatcherSet::compile_all(&[
            "severity = critical".to_string(),
            "env != dev".to_string(),
            "service =~ api-.*".to_string(),
        ])
        .unwrap();
        assert_eq!(set.len(), 3);
        assert!(!set.is_empty());

        assert!(set.matches(&labels(&[
            ("severity", "critical"),
            ("env", "prod"),
            ("service", "api-v1"),
        ])));
        assert!(!set.matches(&labels(&[
            ("severity", "critical"),
            ("env", "dev"),
            ("service", "api-v1"),
        ])));
        assert!(!set.matches(&labels(&[("severity", "critical")])));

        let err = MatcherSet::compile_all(&["ok = 1".to_string(), "bad =~ [".to_string()]);
        assert!(err.is_err());
    }
}
