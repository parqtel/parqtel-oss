use parqtel_core::{Error, LabelSet, Result};
use regex::Regex;

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
                let re = Regex::new(&value)
                    .map_err(|e| Error::Validation(format!("Invalid regex '{}': {}", value, e)))?;
                Some(re)
            }
            _ => None,
        };
        Ok(Self {
            name,
            op,
            value,
            regex,
        })
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
        (
            if name.is_empty() {
                None
            } else {
                Some(name.to_string())
            },
            labels,
        )
    } else {
        (Some(selector.to_string()), "")
    };

    let mut matchers = Vec::new();
    if !labels_part.is_empty() {
        for part in labels_part.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            let (op, split_idx) = if let Some(idx) = part.find("=~") {
                (MatchOp::RegexMatch, idx)
            } else if let Some(idx) = part.find("!~") {
                (MatchOp::RegexNotMatch, idx)
            } else if let Some(idx) = part.find("!=") {
                (MatchOp::NotEqual, idx)
            } else if let Some(idx) = part.find('=') {
                (MatchOp::Equal, idx)
            } else {
                return Err(Error::Validation(format!(
                    "Invalid label matcher: {}",
                    part
                )));
            };

            let name = part[..split_idx].trim().to_string();
            let value = part[split_idx
                + (if op == MatchOp::RegexMatch
                    || op == MatchOp::RegexNotMatch
                    || op == MatchOp::NotEqual
                {
                    2
                } else {
                    1
                })..]
                .trim();

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

pub type ParsedQuery = (
    String,                                   // metric name
    Vec<LabelMatcher>,                        // label matchers
    Option<crate::plan::AggregationOp>,       // aggregation / transform op
    Option<f64>,                              // quantile (histogram_quantile)
    Option<usize>,                            // topk_n / bottomk_n
    Vec<String>,                              // by() labels
    Vec<String>,                              // without() labels
    Option<(String, String, String, String)>, // label_replace params
    Option<f64>,                              // scalar_param (round to_nearest)
    Option<(Option<f64>, Option<f64>)>,       // clamp (min, max)
);

/// Parses a PromQL-style query including all supported aggregation functions.
pub fn parse_query(query: &str) -> Result<ParsedQuery> {
    let query = query.trim();

    // ── histogram_quantile(φ, selector) ────────────────────────────────────
    if let Some(inner) = strip_fn("histogram_quantile", query) {
        let (head, tail) = split_first_arg(inner)?;
        let q: f64 = head
            .trim()
            .parse()
            .map_err(|_| Error::Validation("Invalid quantile value".into()))?;
        let (name, matchers) = parse_selector(tail.trim())?;
        return Ok((
            name.unwrap_or_default(),
            matchers,
            Some(crate::plan::AggregationOp::HistogramQuantile),
            Some(q),
            None,
            vec![],
            vec![],
            None,
            None,
            None,
        ));
    }

    // ── topk(N, selector) / bottomk(N, selector) ───────────────────────────
    for (fname, op) in [
        ("topk", crate::plan::AggregationOp::TopK),
        ("bottomk", crate::plan::AggregationOp::BottomK),
    ] {
        if let Some(inner) = strip_fn(fname, query) {
            let (head, tail) = split_first_arg(inner)?;
            let n: usize = head
                .trim()
                .parse()
                .map_err(|_| Error::Validation(format!("{fname} N must be a positive integer")))?;
            let (selector, by, without) = parse_selector_with_grouping(tail.trim())?;
            let (name, matchers) = parse_selector(selector)?;
            return Ok((
                name.unwrap_or_default(),
                matchers,
                Some(op),
                None,
                Some(n),
                by,
                without,
                None,
                None,
                None,
            ));
        }
    }

    // ── label_replace(selector, dst, replacement, src, regex) ──────────────
    if let Some(inner) = strip_fn("label_replace", query) {
        let args = split_args(inner);
        if args.len() != 5 {
            return Err(Error::Validation(
                "label_replace requires 5 arguments".into(),
            ));
        }
        let (name, matchers) = parse_selector(args[0].trim())?;
        let dst = unquote(args[1].trim()).to_string();
        let repl = unquote(args[2].trim()).to_string();
        let src = unquote(args[3].trim()).to_string();
        let regex = unquote(args[4].trim()).to_string();
        return Ok((
            name.unwrap_or_default(),
            matchers,
            Some(crate::plan::AggregationOp::LabelReplace),
            None,
            None,
            vec![],
            vec![],
            Some((dst, repl, src, regex)),
            None,
            None,
        ));
    }

    // ── clamp_min(selector, min) / clamp_max(selector, max) ────────────────
    if let Some(inner) = strip_fn("clamp_min", query) {
        let (selector, tail) = split_first_selector(inner)?;
        let min: f64 = tail
            .trim()
            .parse()
            .map_err(|_| Error::Validation("clamp_min: invalid min value".into()))?;
        let (name, matchers) = parse_selector(selector)?;
        return Ok((
            name.unwrap_or_default(),
            matchers,
            Some(crate::plan::AggregationOp::ClampMin),
            None,
            None,
            vec![],
            vec![],
            None,
            None,
            Some((Some(min), None)),
        ));
    }
    if let Some(inner) = strip_fn("clamp_max", query) {
        let (selector, tail) = split_first_selector(inner)?;
        let max: f64 = tail
            .trim()
            .parse()
            .map_err(|_| Error::Validation("clamp_max: invalid max value".into()))?;
        let (name, matchers) = parse_selector(selector)?;
        return Ok((
            name.unwrap_or_default(),
            matchers,
            Some(crate::plan::AggregationOp::ClampMax),
            None,
            None,
            vec![],
            vec![],
            None,
            None,
            Some((None, Some(max))),
        ));
    }

    // ── round(selector[, to_nearest]) ──────────────────────────────────────
    if let Some(inner) = strip_fn("round", query) {
        // round has optional second arg
        let (selector, scalar) = if inner.contains(',') {
            let (sel, tail) = split_first_selector(inner)?;
            let s: f64 = tail
                .trim()
                .parse()
                .map_err(|_| Error::Validation("round: invalid to_nearest value".into()))?;
            (sel, Some(s))
        } else {
            (inner, None)
        };
        let (name, matchers) = parse_selector(selector.trim())?;
        return Ok((
            name.unwrap_or_default(),
            matchers,
            Some(crate::plan::AggregationOp::Round),
            None,
            None,
            vec![],
            vec![],
            None,
            scalar,
            None,
        ));
    }

    // ── simple one-arg transforms: abs, ceil, floor ─────────────────────────
    for (fname, op) in [
        ("abs", crate::plan::AggregationOp::Abs),
        ("ceil", crate::plan::AggregationOp::Ceil),
        ("floor", crate::plan::AggregationOp::Floor),
    ] {
        if let Some(inner) = strip_fn(fname, query) {
            let (name, matchers) = parse_selector(inner.trim())?;
            return Ok((
                name.unwrap_or_default(),
                matchers,
                Some(op),
                None,
                None,
                vec![],
                vec![],
                None,
                None,
                None,
            ));
        }
    }

    // ── range functions: rate, irate, increase, delta ──────────────────────
    for (fname, op) in [
        ("irate", crate::plan::AggregationOp::Irate),
        ("increase", crate::plan::AggregationOp::Increase),
        ("delta", crate::plan::AggregationOp::Delta),
        ("rate", crate::plan::AggregationOp::Rate),
    ] {
        if let Some(inner) = strip_fn(fname, query) {
            // Strip optional range selector [Xm]
            let sel = strip_range(inner);
            let (name, matchers) = parse_selector(sel)?;
            return Ok((
                name.unwrap_or_default(),
                matchers,
                Some(op),
                None,
                None,
                vec![],
                vec![],
                None,
                None,
                None,
            ));
        }
    }

    // ── standard aggregations with optional by/without ──────────────────────
    for (fname, op) in [
        ("avg", crate::plan::AggregationOp::Avg),
        ("sum", crate::plan::AggregationOp::Sum),
        ("min", crate::plan::AggregationOp::Min),
        ("max", crate::plan::AggregationOp::Max),
        ("count", crate::plan::AggregationOp::Count),
        ("stddev", crate::plan::AggregationOp::Stddev),
        ("stdvar", crate::plan::AggregationOp::Stdvar),
    ] {
        if let Some(inner) = strip_fn(fname, query) {
            let (selector, by, without) = parse_selector_with_grouping(inner)?;
            let (name, matchers) = parse_selector(selector)?;
            return Ok((
                name.unwrap_or_default(),
                matchers,
                Some(op),
                None,
                None,
                by,
                without,
                None,
                None,
                None,
            ));
        }
        // Also handle `sum by (label) (selector)` form
        let by_prefix = format!("{fname} by (");
        let without_prefix = format!("{fname} without (");
        if query.starts_with(&by_prefix) || query.starts_with(&without_prefix) {
            let (grouping_labels, is_without, remainder) = parse_leading_grouping(query, fname)?;
            // remainder should be (selector)
            let inner = remainder
                .trim()
                .trim_start_matches('(')
                .trim_end_matches(')');
            let (name, matchers) = parse_selector(inner)?;
            let (by, without) = if is_without {
                (vec![], grouping_labels)
            } else {
                (grouping_labels, vec![])
            };
            return Ok((
                name.unwrap_or_default(),
                matchers,
                Some(op),
                None,
                None,
                by,
                without,
                None,
                None,
                None,
            ));
        }
    }

    // ── bare selector ───────────────────────────────────────────────────────
    let (name, matchers) = parse_selector(query)?;
    Ok((
        name.unwrap_or_default(),
        matchers,
        None,
        None,
        None,
        vec![],
        vec![],
        None,
        None,
        None,
    ))
}

// ── helpers ─────────────────────────────────────────────────────────────────

/// Strips `fn_name(` prefix and `)` suffix, returning the inner string or None.
fn strip_fn<'a>(fname: &str, query: &'a str) -> Option<&'a str> {
    let prefix = format!("{fname}(");
    if query.starts_with(prefix.as_str()) && query.ends_with(')') {
        Some(&query[prefix.len()..query.len() - 1])
    } else {
        None
    }
}

/// Strips trailing `[Xm]` or `[Xs]` range selector.
fn strip_range(s: &str) -> &str {
    if let Some(idx) = s.rfind('[') {
        s[..idx].trim()
    } else {
        s.trim()
    }
}

/// Splits `"N, rest"` into `("N", "rest")` at the first comma.
fn split_first_arg(s: &str) -> Result<(&str, &str)> {
    s.find(',')
        .map(|i| (&s[..i], &s[i + 1..]))
        .ok_or_else(|| Error::Validation("Expected comma-separated arguments".into()))
}

/// Splits `"selector, scalar"` by finding the closing `}` or end of bare name,
/// then the first comma after that.
fn split_first_selector(s: &str) -> Result<(&str, &str)> {
    // Find end of selector: after `}` if present, else first comma
    let after_sel = if let Some(brace) = s.find('}') {
        brace + 1
    } else if let Some(comma) = s.find(',') {
        comma
    } else {
        return Err(Error::Validation(
            "Expected two comma-separated arguments".into(),
        ));
    };
    // Now find the comma after the selector end
    s[after_sel..]
        .find(',')
        .map(|i| (&s[..after_sel + i], s[after_sel + i + 1..].trim()))
        .ok_or_else(|| Error::Validation("Expected comma after selector".into()))
}

/// Splits a comma-separated argument list, respecting nested `{}` and `()`.
fn split_args(s: &str) -> Vec<&str> {
    let mut args = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' | '{' | '[' => depth += 1,
            ')' | '}' | ']' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                args.push(s[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    args.push(s[start..].trim());
    args
}

/// Strips double or single quotes from a string.
fn unquote(s: &str) -> &str {
    if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

/// Parses optional ` by (l1, l2)` or ` without (l1, l2)` suffix from the inner
/// part of an aggregation, returning (selector_str, by_labels, without_labels).
fn parse_selector_with_grouping(inner: &str) -> Result<(&str, Vec<String>, Vec<String>)> {
    // Check for ` by (...)` or ` without (...)` embedded inside the parens
    // e.g. inner = `http_requests{} by (method, status)`
    if let Some(by_idx) = find_keyword(inner, " by (") {
        let selector = inner[..by_idx].trim();
        let labels_str = &inner[by_idx + 5..]; // skip " by ("
        let close = labels_str.find(')').unwrap_or(labels_str.len());
        let labels = parse_label_list(&labels_str[..close]);
        return Ok((selector, labels, vec![]));
    }
    if let Some(wo_idx) = find_keyword(inner, " without (") {
        let selector = inner[..wo_idx].trim();
        let labels_str = &inner[wo_idx + 10..]; // skip " without ("
        let close = labels_str.find(')').unwrap_or(labels_str.len());
        let labels = parse_label_list(&labels_str[..close]);
        return Ok((selector, vec![], labels));
    }
    Ok((inner, vec![], vec![]))
}

/// Parses leading `agg by/without (labels) (selector)` form.
/// Returns (labels, is_without, remainder_after_labels_paren).
fn parse_leading_grouping<'a>(query: &'a str, fname: &str) -> Result<(Vec<String>, bool, &'a str)> {
    // e.g. "sum by (service) (metric{...})"
    let is_without = query.contains(" without (");
    let keyword = if is_without { " without (" } else { " by (" };
    let skip_prefix = fname.len() + keyword.len();
    let rest = &query[skip_prefix..];
    let close = rest
        .find(')')
        .ok_or_else(|| Error::Validation("Missing closing ) in grouping clause".into()))?;
    let labels = parse_label_list(&rest[..close]);
    Ok((labels, is_without, rest[close + 1..].trim()))
}

fn find_keyword(s: &str, kw: &str) -> Option<usize> {
    s.find(kw)
}

fn parse_label_list(s: &str) -> Vec<String> {
    s.split(',')
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
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
        let (name, matchers) =
            parse_selector("http_requests_total{method=\"GET\", status!=\"500\"}").unwrap();
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
        let (name, _, agg, q, topk, by, without, lr, scalar, clamp) =
            parse_query("avg(cpu_usage{host=\"h1\"})").unwrap();
        assert_eq!(name, "cpu_usage");
        assert_eq!(agg, Some(crate::plan::AggregationOp::Avg));
        assert!(q.is_none());
        assert!(topk.is_none());
        assert!(by.is_empty());
        assert!(without.is_empty());
        assert!(lr.is_none());
        assert!(scalar.is_none());
        assert!(clamp.is_none());

        let (name, _, agg, q, ..) = parse_query("histogram_quantile(0.9, latency)").unwrap();
        assert_eq!(name, "latency");
        assert_eq!(agg, Some(crate::plan::AggregationOp::HistogramQuantile));
        assert_eq!(q, Some(0.9));

        let (name, _, agg, ..) = parse_query("rate(http_requests[5m])").unwrap();
        assert_eq!(name, "http_requests");
        assert_eq!(agg, Some(crate::plan::AggregationOp::Rate));

        let (name, _, agg, ..) = parse_query("irate(http_requests[1m])").unwrap();
        assert_eq!(name, "http_requests");
        assert_eq!(agg, Some(crate::plan::AggregationOp::Irate));

        let (name, _, agg, ..) = parse_query("increase(http_requests[5m])").unwrap();
        assert_eq!(name, "http_requests");
        assert_eq!(agg, Some(crate::plan::AggregationOp::Increase));

        let (name, _, agg, ..) = parse_query("delta(temperature[10m])").unwrap();
        assert_eq!(name, "temperature");
        assert_eq!(agg, Some(crate::plan::AggregationOp::Delta));

        let (name, _, agg, ..) = parse_query("stddev(cpu)").unwrap();
        assert_eq!(name, "cpu");
        assert_eq!(agg, Some(crate::plan::AggregationOp::Stddev));

        // topk / bottomk
        let (name, _, agg, _, n, ..) = parse_query("topk(5, http_requests)").unwrap();
        assert_eq!(name, "http_requests");
        assert_eq!(agg, Some(crate::plan::AggregationOp::TopK));
        assert_eq!(n, Some(5));

        let (name, _, agg, _, n, ..) = parse_query("bottomk(3, errors_total)").unwrap();
        assert_eq!(name, "errors_total");
        assert_eq!(agg, Some(crate::plan::AggregationOp::BottomK));
        assert_eq!(n, Some(3));

        // instant transforms
        let (name, _, agg, ..) = parse_query("abs(cpu)").unwrap();
        assert_eq!(name, "cpu");
        assert_eq!(agg, Some(crate::plan::AggregationOp::Abs));

        let (_name, _, agg, ..) = parse_query("ceil(cpu)").unwrap();
        assert_eq!(agg, Some(crate::plan::AggregationOp::Ceil));
        let (name2, _, agg2, ..) = parse_query("floor(cpu)").unwrap();
        assert_eq!(name2, "cpu");
        assert_eq!(agg2, Some(crate::plan::AggregationOp::Floor));

        // round with optional to_nearest
        let (_, _, agg, _, _, _, _, _, scalar, _) = parse_query("round(cpu, 0.5)").unwrap();
        assert_eq!(agg, Some(crate::plan::AggregationOp::Round));
        assert_eq!(scalar, Some(0.5));

        // clamp_min / clamp_max
        let (_, _, agg, _, _, _, _, _, _, clamp) = parse_query("clamp_min(cpu, 0.0)").unwrap();
        assert_eq!(agg, Some(crate::plan::AggregationOp::ClampMin));
        assert_eq!(clamp, Some((Some(0.0), None)));

        let (_, _, agg, _, _, _, _, _, _, clamp) = parse_query("clamp_max(cpu, 100.0)").unwrap();
        assert_eq!(agg, Some(crate::plan::AggregationOp::ClampMax));
        assert_eq!(clamp, Some((None, Some(100.0))));

        // sum with by grouping
        let (name, _, agg, _, _, by, without, ..) =
            parse_query("sum(http_requests{} by (method, status))").unwrap();
        assert_eq!(name, "http_requests");
        assert_eq!(agg, Some(crate::plan::AggregationOp::Sum));
        assert_eq!(by, vec!["method", "status"]);
        assert!(without.is_empty());

        // label_replace
        let (name, _, agg, _, _, _, _, lr, ..) =
            parse_query("label_replace(cpu, \"host_short\", \"$1\", \"host\", \"([^.]+).*\")")
                .unwrap();
        assert_eq!(name, "cpu");
        assert_eq!(agg, Some(crate::plan::AggregationOp::LabelReplace));
        let lr = lr.unwrap();
        assert_eq!(lr.0, "host_short");
        assert_eq!(lr.2, "host");
    }

    #[test]
    fn test_invalid_selector() {
        assert!(parse_selector("").is_err());
        assert!(parse_selector("m{a=").is_err());
        assert!(parse_selector("m{a").is_err());
    }
}
