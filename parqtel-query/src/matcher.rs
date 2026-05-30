use regex::Regex;
use parqtel_core::{Error, Result, LabelSet};

/// Operators for label matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOp {
    /// Equal (=)
    Equal,
    /// Not Equal (!=)
    NotEqual,
    /// Regex Match (=~)
    RegexMatch,
    /// Regex Not Match (!~)
    RegexNotMatch,
}

/// A single label matcher predicate.
#[derive(Debug, Clone)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
    pub regex: Option<Regex>,
}

impl LabelMatcher {
    /// Creates a new [LabelMatcher] with Equal operator.
    pub fn equal(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op: MatchOp::Equal,
            value: value.into(),
            regex: None,
        }
    }

    /// Creates a new [LabelMatcher] and compiles regex if needed.
    pub fn new(name: String, op: MatchOp, value: String) -> Result<Self> {
        let regex = match op {
            MatchOp::RegexMatch | MatchOp::RegexNotMatch => {
                let re = Regex::new(&value).map_err(|e| Error::Validation(format!("Invalid regex '{}': {}", value, e)))?;
                Some(re)
            }
            _ => None,
        };
        Ok(Self { name, op, value, regex })
    }

    /// Evaluates this matcher against a label value.
    pub fn matches(&self, label_value: Option<&str>) -> bool {
        let val = label_value.unwrap_or("");
        match self.op {
            MatchOp::Equal => val == self.value,
            MatchOp::NotEqual => val != self.value,
            MatchOp::RegexMatch => self.regex.as_ref().is_some_and(|re| re.is_match(val)),
            MatchOp::RegexNotMatch => self.regex.as_ref().is_some_and(|re| !re.is_match(val)),
        }
    }
}

/// Evaluates a list of matchers against a set of labels and a metric name.
pub fn evaluate_matchers(matchers: &[LabelMatcher], labels: &LabelSet, metric_name: &str) -> bool {
    for m in matchers {
        let val = if m.name == "__name__" {
            Some(metric_name)
        } else {
            labels.get(&m.name)
        };
        if !m.matches(val) {
            return false;
        }
    }
    true
}

/// Parses a Prometheus-style selector like `http_requests_total{method="GET"}`.
pub fn parse_selector(selector: &str) -> Result<(Option<String>, Vec<LabelMatcher>)> {
    let selector = selector.trim();
    if selector.is_empty() {
        return Err(Error::Validation("Empty selector".into()));
    }

    let (metric_name, labels_part) = if let Some(start) = selector.find('{') {
        if !selector.ends_with('}') {
            return Err(Error::Validation("Selector missing closing brace".into()));
        }
        let name = selector[..start].trim();
        let labels = &selector[start + 1..selector.len() - 1];
        (if name.is_empty() { None } else { Some(name.to_string()) }, labels)
    } else {
        (Some(selector.to_string()), "")
    };

    let mut matchers = Vec::new();
    if !labels_part.is_empty() {
        for part in labels_part.split(',') {
            let part = part.trim();
            if part.is_empty() { continue; }
            
            let (op, split_idx) = if let Some(idx) = part.find("=~") {
                (MatchOp::RegexMatch, idx)
            } else if let Some(idx) = part.find("!~") {
                (MatchOp::RegexNotMatch, idx)
            } else if let Some(idx) = part.find("!=") {
                (MatchOp::NotEqual, idx)
            } else if let Some(idx) = part.find('=') {
                (MatchOp::Equal, idx)
            } else {
                return Err(Error::Validation(format!("Invalid label matcher: {}", part)));
            };

            let name = part[..split_idx].trim().to_string();
            let value = part[split_idx + (if op == MatchOp::RegexMatch || op == MatchOp::RegexNotMatch || op == MatchOp::NotEqual { 2 } else { 1 })..].trim();
            
            // Strip quotes
            let value = if value.starts_with('"') && value.ends_with('"') {
                &value[1..value.len() - 1]
            } else {
                value
            };

            matchers.push(LabelMatcher::new(name, op, value.to_string())?);
        }
    }

    Ok((metric_name, matchers))
}

pub type ParsedQuery = (String, Vec<LabelMatcher>, Option<crate::plan::AggregationOp>, Option<f64>);

/// Parses a PromQL-style query including basic aggregations.
pub fn parse_query(query: &str) -> Result<ParsedQuery> {
    let query = query.trim();
    
    // Support histogram_quantile(0.95, metric{...})
    if query.starts_with("histogram_quantile(") && query.ends_with(')') {
        let inner = &query[19..query.len() - 1];
        let parts: Vec<&str> = inner.splitn(2, ',').collect();
        if parts.len() != 2 {
            return Err(Error::Validation("histogram_quantile requires quantile and series".into()));
        }
        let q: f64 = parts[0].trim().parse().map_err(|_| Error::Validation("Invalid quantile".into()))?;
        let (name, matchers) = parse_selector(parts[1])?;
        return Ok((name.unwrap_or_default(), matchers, Some(crate::plan::AggregationOp::HistogramQuantile), Some(q)));
    }

    // Support standard aggregations: avg, sum, min, max, count, rate
    let (agg, inner) = if query.starts_with("avg(") && query.ends_with(')') {
        (Some(crate::plan::AggregationOp::Avg), &query[4..query.len() - 1])
    } else if query.starts_with("sum(") && query.ends_with(')') {
        (Some(crate::plan::AggregationOp::Sum), &query[4..query.len() - 1])
    } else if query.starts_with("min(") && query.ends_with(')') {
        (Some(crate::plan::AggregationOp::Min), &query[4..query.len() - 1])
    } else if query.starts_with("max(") && query.ends_with(')') {
        (Some(crate::plan::AggregationOp::Max), &query[4..query.len() - 1])
    } else if query.starts_with("count(") && query.ends_with(')') {
        (Some(crate::plan::AggregationOp::Count), &query[6..query.len() - 1])
    } else if query.starts_with("rate(") && query.ends_with(')') {
        // Handle rate(metric[5m]) - strip range selector for now as parqtel handles windowing via step
        let mut inner = &query[5..query.len() - 1];
        if let Some(idx) = inner.find('[') {
            inner = &inner[..idx];
        }
        (Some(crate::plan::AggregationOp::Rate), inner)
    } else {
        (None, query)
    };

    let (name, matchers) = parse_selector(inner)?;
    Ok((name.unwrap_or_default(), matchers, agg, None))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn test_label_matching() {
        let m = LabelMatcher::new("method".into(), MatchOp::Equal, "GET".into()).unwrap();
        assert!(m.matches(Some("GET")));
        assert!(!m.matches(Some("POST")));
        assert!(!m.matches(None));

        let m2 = LabelMatcher::new("code".into(), MatchOp::NotEqual, "500".into()).unwrap();
        assert!(m2.matches(Some("200")));
        assert!(!m2.matches(Some("500")));
        assert!(m2.matches(None)); // None != "500" is true

        let m3 = LabelMatcher::new("host".into(), MatchOp::RegexMatch, "api-.*".into()).unwrap();
        assert!(m3.matches(Some("api-v1")));
        assert!(!m3.matches(Some("web-v1")));

        let m4 = LabelMatcher::new("host".into(), MatchOp::RegexNotMatch, "api-.*".into()).unwrap();
        assert!(!m4.matches(Some("api-v1")));
        assert!(m4.matches(Some("web-v1")));
    }

    #[test]
    fn test_selector_parsing() {
        let (name, matchers) = parse_selector("http_requests_total{method=\"GET\", status!=\"500\"}").unwrap();
        assert_eq!(name, Some("http_requests_total".into()));
        assert_eq!(matchers.len(), 2);
        assert_eq!(matchers[0].name, "method");
        assert_eq!(matchers[0].op, MatchOp::Equal);
        assert_eq!(matchers[1].name, "status");
        assert_eq!(matchers[1].op, MatchOp::NotEqual);
        
        // Regex and quotes
        let (_, matchers) = parse_selector("m{a=~\"v.*\", b!~'x.*'}").unwrap();
        assert_eq!(matchers[0].op, MatchOp::RegexMatch);
        assert_eq!(matchers[0].value, "v.*");
        assert_eq!(matchers[1].op, MatchOp::RegexNotMatch);
        assert_eq!(matchers[1].value, "'x.*'"); // Single quotes not stripped by current logic, only double
    }

    #[test]
    fn test_parse_query() {
        let (name, _, agg, q) = parse_query("avg(cpu_usage{host=\"h1\"})").unwrap();
        assert_eq!(name, "cpu_usage");
        assert_eq!(agg, Some(crate::plan::AggregationOp::Avg));
        assert!(q.is_none());

        let (name, _, agg, q) = parse_query("histogram_quantile(0.9, latency)").unwrap();
        assert_eq!(name, "latency");
        assert_eq!(agg, Some(crate::plan::AggregationOp::HistogramQuantile));
        assert_eq!(q, Some(0.9));
        
        let (name, _, agg, _) = parse_query("rate(http_requests[5m])").unwrap();
        assert_eq!(name, "http_requests");
        assert_eq!(agg, Some(crate::plan::AggregationOp::Rate));
    }

    #[test]
    fn test_invalid_selector() {
        assert!(parse_selector("").is_err());
        assert!(parse_selector("m{a=").is_err());
        assert!(parse_selector("m{a").is_err());
    }
}
