use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

const REDACTED: &str = "***REDACTED***";

/// Top-level `[alerting]` configuration section.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AlertingConfig {
    /// Alertmanager-compatible routing, grouping, inhibition, and receivers.
    #[serde(default)]
    pub routing: Option<AlertmanagerRoutingConfig>,
}

/// Alertmanager-compatible routing configuration (`[alerting.routing]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertmanagerRoutingConfig {
    #[serde(default)]
    pub global: RoutingGlobalConfig,
    /// Maximum number of active alert groups held in memory before evicting
    /// oldest resolved/idle groups.
    #[serde(default = "default_max_active_groups")]
    pub max_active_groups: usize,
    /// Hierarchical route tree.
    #[serde(default)]
    pub route: Option<AlertRouteConfig>,
    /// Inhibition rules suppressing lower-priority alerts when a source fires.
    #[serde(default)]
    pub inhibit_rules: Vec<InhibitRuleConfig>,
    /// Named receiver definitions (Slack, PagerDuty, Teams, generic webhook).
    #[serde(default)]
    pub receivers: Vec<ReceiverConfig>,
}

impl Default for AlertmanagerRoutingConfig {
    fn default() -> Self {
        Self {
            global: RoutingGlobalConfig::default(),
            max_active_groups: default_max_active_groups(),
            route: None,
            inhibit_rules: Vec::new(),
            receivers: Vec::new(),
        }
    }
}

fn default_max_active_groups() -> usize {
    10_000
}

/// Global defaults for Alertmanager routing and delivery retries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingGlobalConfig {
    #[serde(default = "default_resolve_timeout")]
    pub resolve_timeout: String,
    #[serde(default = "default_http_timeout")]
    pub http_timeout: String,
    #[serde(default = "default_retry_initial_backoff")]
    pub retry_initial_backoff: String,
    #[serde(default = "default_retry_max_backoff")]
    pub retry_max_backoff: String,
    #[serde(default = "default_retry_max_attempts")]
    pub retry_max_attempts: usize,
}

impl Default for RoutingGlobalConfig {
    fn default() -> Self {
        Self {
            resolve_timeout: default_resolve_timeout(),
            http_timeout: default_http_timeout(),
            retry_initial_backoff: default_retry_initial_backoff(),
            retry_max_backoff: default_retry_max_backoff(),
            retry_max_attempts: default_retry_max_attempts(),
        }
    }
}

fn default_resolve_timeout() -> String {
    "5m".to_string()
}
fn default_http_timeout() -> String {
    "10s".to_string()
}
fn default_retry_initial_backoff() -> String {
    "1s".to_string()
}
fn default_retry_max_backoff() -> String {
    "30s".to_string()
}
fn default_retry_max_attempts() -> usize {
    3
}

/// A node in the hierarchical Alertmanager route tree.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AlertRouteConfig {
    /// Alertmanager matchers, e.g. `["severity = critical", "service.name =~ api-.*"]`.
    #[serde(default)]
    pub matchers: Vec<String>,
    /// Whether to continue evaluating subsequent sibling routes after a match.
    #[serde(rename = "continue", default)]
    pub continue_matching: bool,
    /// Label names to group alerts by. Use `["..."]` to disable grouping.
    #[serde(default)]
    pub group_by: Option<Vec<String>>,
    /// Initial wait before sending the first notification for a new group.
    #[serde(default)]
    pub group_wait: Option<String>,
    /// Minimum interval between notifications for new alerts in an active group.
    #[serde(default)]
    pub group_interval: Option<String>,
    /// Minimum interval before re-notifying an unchanged firing group.
    #[serde(default)]
    pub repeat_interval: Option<String>,
    /// Target receiver name (must exist in `receivers`).
    #[serde(default)]
    pub receiver: Option<String>,
    /// Child routes evaluated depth-first if this route matches.
    #[serde(default)]
    pub routes: Vec<AlertRouteConfig>,
}

/// Inhibition rule suppressing `target_matchers` when an active firing alert
/// matches `source_matchers` with identical values for all `equal` label keys.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InhibitRuleConfig {
    #[serde(default)]
    pub source_matchers: Vec<String>,
    #[serde(default)]
    pub target_matchers: Vec<String>,
    #[serde(default)]
    pub equal: Vec<String>,
}

/// A named notification receiver with one or more channel configurations.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReceiverConfig {
    pub name: String,
    #[serde(default)]
    pub slack_configs: Vec<SlackReceiverConfig>,
    #[serde(default)]
    pub pagerduty_configs: Vec<PagerDutyReceiverConfig>,
    #[serde(default)]
    pub teams_configs: Vec<TeamsReceiverConfig>,
    #[serde(default)]
    pub webhook_configs: Vec<WebhookReceiverConfig>,
}

/// Slack incoming-webhook receiver configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SlackReceiverConfig {
    pub webhook_url: String,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub icon_emoji: Option<String>,
    #[serde(default)]
    pub send_resolved: bool,
}

/// PagerDuty Events API v2 receiver configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PagerDutyReceiverConfig {
    pub routing_key: String,
    /// Override endpoint URL (defaults to `https://events.pagerduty.com/v2/enqueue`).
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub severity: Option<String>,
    #[serde(default)]
    pub send_resolved: bool,
}

/// Microsoft Teams incoming-webhook / Workflow Adaptive Card receiver configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TeamsReceiverConfig {
    pub webhook_url: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub send_resolved: bool,
}

/// Generic HTTP webhook receiver configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WebhookReceiverConfig {
    pub url: String,
    #[serde(default)]
    pub send_resolved: bool,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

/// Supported Alertmanager label matcher operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchOp {
    Eq,
    Ne,
    Regex,
    NotRegex,
}

/// Parsed label matcher before regex compilation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatcherSpec {
    pub label: String,
    pub op: MatchOp,
    pub value: String,
}

/// Parse an Alertmanager matcher expression (`=`, `!=`, `=~`, `!~`).
pub fn parse_matcher_str(input: &str) -> std::result::Result<MatcherSpec, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("matcher cannot be empty".to_string());
    }

    // Find the earliest operator occurrence; check 2-char operators before `=`.
    let candidates = [
        ("=~", MatchOp::Regex),
        ("!~", MatchOp::NotRegex),
        ("!=", MatchOp::Ne),
        ("=", MatchOp::Eq),
    ];

    let mut best: Option<(usize, usize, MatchOp)> = None;
    for (sym, op) in candidates {
        if let Some(idx) = trimmed.find(sym) {
            let replace = match best {
                None => true,
                Some((best_idx, best_len, _)) => {
                    idx < best_idx || (idx == best_idx && sym.len() > best_len)
                }
            };
            if replace {
                best = Some((idx, sym.len(), op));
            }
        }
    }

    let (idx, op_len, op) =
        best.ok_or_else(|| format!("matcher '{trimmed}' missing operator (=, !=, =~, !~)"))?;

    let label = trimmed[..idx].trim();
    if label.is_empty() {
        return Err(format!("matcher '{trimmed}' has empty label name"));
    }

    let raw_val = trimmed[idx + op_len..].trim();
    let value = strip_quotes(raw_val);

    if matches!(op, MatchOp::Regex | MatchOp::NotRegex) {
        let anchored = format!("^(?:{value})$");
        Regex::new(&anchored)
            .map_err(|e| format!("invalid regex '{value}' in matcher '{trimmed}': {e}"))?;
    }

    Ok(MatcherSpec {
        label: label.to_string(),
        op,
        value: value.to_string(),
    })
}

fn strip_quotes(s: &str) -> &str {
    if s.len() >= 2 {
        let bytes = s.as_bytes();
        if (bytes[0] == b'"' && bytes[s.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[s.len() - 1] == b'\'')
        {
            return &s[1..s.len() - 1];
        }
    }
    s
}

/// Parse a Prometheus/Alertmanager duration string (e.g. `500ms`, `30s`, `5m`, `4h`, `1d`, `1h30m`, or integer seconds).
pub fn parse_duration(input: &str) -> std::result::Result<Duration, String> {
    let s = input.trim();
    if s.is_empty() {
        return Err("duration cannot be empty".to_string());
    }
    if let Ok(secs) = s.parse::<u64>() {
        return Ok(Duration::from_secs(secs));
    }

    let mut total_ms: u128 = 0;
    let mut rest = s;
    while !rest.is_empty() {
        let num_end = rest
            .find(|c: char| !c.is_ascii_digit())
            .ok_or_else(|| format!("duration '{input}' missing unit"))?;
        if num_end == 0 {
            return Err(format!("invalid duration '{input}'"));
        }
        let num: u128 = rest[..num_end]
            .parse()
            .map_err(|_| format!("invalid number in duration '{input}'"))?;
        rest = &rest[num_end..];

        let unit_end = rest
            .find(|c: char| c.is_ascii_digit())
            .unwrap_or(rest.len());
        let unit = &rest[..unit_end];
        rest = &rest[unit_end..];

        let mult_ms: u128 = match unit {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60 * 1_000,
            "h" => 3_600 * 1_000,
            "d" => 86_400 * 1_000,
            "w" => 7 * 86_400 * 1_000,
            _ => return Err(format!("unknown duration unit '{unit}' in '{input}'")),
        };
        total_ms = total_ms
            .checked_add(
                num.checked_mul(mult_ms)
                    .ok_or_else(|| format!("duration '{input}' overflows"))?,
            )
            .ok_or_else(|| format!("duration '{input}' overflows"))?;
    }

    let millis =
        u64::try_from(total_ms).map_err(|_| format!("duration '{input}' overflows u64"))?;
    Ok(Duration::from_millis(millis))
}

/// Resolve `${ENV_VAR}` and `{env:ENV_VAR}` placeholders using process environment variables.
pub fn resolve_secret(raw: &str) -> std::result::Result<String, String> {
    resolve_secret_with(raw, |name| std::env::var(name))
}

/// Resolve `${ENV_VAR}` and `{env:ENV_VAR}` placeholders with a custom lookup function.
pub fn resolve_secret_with<F>(raw: &str, lookup: F) -> std::result::Result<String, String>
where
    F: Fn(&str) -> std::result::Result<String, std::env::VarError>,
{
    let mut out = String::with_capacity(raw.len());
    let mut i = 0;
    let bytes = raw.as_bytes();
    while i < bytes.len() {
        if raw[i..].starts_with("${") {
            let start = i + 2;
            let end = raw[start..]
                .find('}')
                .ok_or_else(|| format!("unclosed '${{' placeholder in '{raw}'"))?;
            let var_name = raw[start..start + end].trim();
            if var_name.is_empty() {
                return Err(format!("empty env placeholder in '{raw}'"));
            }
            let val = lookup(var_name)
                .map_err(|_| format!("environment variable '{var_name}' is not set"))?;
            out.push_str(&val);
            i = start + end + 1;
        } else if raw[i..].starts_with("{env:") {
            let start = i + 5;
            let end = raw[start..]
                .find('}')
                .ok_or_else(|| format!("unclosed '{{env:' placeholder in '{raw}'"))?;
            let var_name = raw[start..start + end].trim();
            if var_name.is_empty() {
                return Err(format!("empty env placeholder in '{raw}'"));
            }
            let val = lookup(var_name)
                .map_err(|_| format!("environment variable '{var_name}' is not set"))?;
            out.push_str(&val);
            i = start + end + 1;
        } else {
            let ch = raw[i..]
                .chars()
                .next()
                .ok_or_else(|| "unexpected end of string".to_string())?;
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    Ok(out)
}

impl AlertmanagerRoutingConfig {
    /// Validate the full routing configuration and append human-readable errors.
    pub fn validate_into(&self, prefix: &str, errors: &mut Vec<String>) {
        if self.max_active_groups == 0 {
            errors.push(format!("{prefix}.max_active_groups must be > 0"));
        }

        for (field, val) in [
            ("resolve_timeout", &self.global.resolve_timeout),
            ("http_timeout", &self.global.http_timeout),
            ("retry_initial_backoff", &self.global.retry_initial_backoff),
            ("retry_max_backoff", &self.global.retry_max_backoff),
        ] {
            if let Err(e) = parse_duration(val) {
                errors.push(format!("{prefix}.global.{field}: {e}"));
            }
        }

        let mut receiver_names = HashSet::new();
        for (idx, recv) in self.receivers.iter().enumerate() {
            if recv.name.trim().is_empty() {
                errors.push(format!("{prefix}.receivers[{idx}].name cannot be empty"));
            } else if !receiver_names.insert(recv.name.clone()) {
                errors.push(format!(
                    "{prefix}.receivers[{idx}]: duplicate receiver name '{}'",
                    recv.name
                ));
            }
        }

        if let Some(ref route) = self.route {
            Self::validate_route_node(
                route,
                &format!("{prefix}.route"),
                &receiver_names,
                false,
                errors,
            );
        }

        for (idx, rule) in self.inhibit_rules.iter().enumerate() {
            for (m_idx, m) in rule.source_matchers.iter().enumerate() {
                if let Err(e) = parse_matcher_str(m) {
                    errors.push(format!(
                        "{prefix}.inhibit_rules[{idx}].source_matchers[{m_idx}]: {e}"
                    ));
                }
            }
            for (m_idx, m) in rule.target_matchers.iter().enumerate() {
                if let Err(e) = parse_matcher_str(m) {
                    errors.push(format!(
                        "{prefix}.inhibit_rules[{idx}].target_matchers[{m_idx}]: {e}"
                    ));
                }
            }
        }
    }

    fn validate_route_node(
        route: &AlertRouteConfig,
        path: &str,
        receivers: &HashSet<String>,
        has_inherited_receiver: bool,
        errors: &mut Vec<String>,
    ) {
        for (idx, m) in route.matchers.iter().enumerate() {
            if let Err(e) = parse_matcher_str(m) {
                errors.push(format!("{path}.matchers[{idx}]: {e}"));
            }
        }

        for (field, opt) in [
            ("group_wait", &route.group_wait),
            ("group_interval", &route.group_interval),
            ("repeat_interval", &route.repeat_interval),
        ] {
            if let Some(val) = opt {
                if let Err(e) = parse_duration(val) {
                    errors.push(format!("{path}.{field}: {e}"));
                }
            }
        }

        let has_receiver = if let Some(ref recv) = route.receiver {
            if !receivers.contains(recv) {
                errors.push(format!(
                    "{path}.receiver '{recv}' does not match any defined receiver"
                ));
            }
            true
        } else {
            has_inherited_receiver
        };

        if route.routes.is_empty() && !has_receiver {
            errors.push(format!(
                "{path} has no receiver and no inherited parent receiver"
            ));
        }

        for (idx, child) in route.routes.iter().enumerate() {
            Self::validate_route_node(
                child,
                &format!("{path}.routes[{idx}]"),
                receivers,
                has_receiver,
                errors,
            );
        }
    }

    /// Return a copy of this configuration with all secrets, tokens, and webhook URLs redacted.
    pub fn redacted(&self) -> Self {
        let mut clone = self.clone();
        for recv in &mut clone.receivers {
            for s in &mut recv.slack_configs {
                if !s.webhook_url.is_empty() {
                    s.webhook_url = REDACTED.to_string();
                }
            }
            for pd in &mut recv.pagerduty_configs {
                if !pd.routing_key.is_empty() {
                    pd.routing_key = REDACTED.to_string();
                }
            }
            for t in &mut recv.teams_configs {
                if !t.webhook_url.is_empty() {
                    t.webhook_url = REDACTED.to_string();
                }
            }
            for w in &mut recv.webhook_configs {
                if !w.url.is_empty() {
                    w.url = REDACTED.to_string();
                }
                for val in w.headers.values_mut() {
                    *val = REDACTED.to_string();
                }
            }
        }
        clone
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn test_parse_duration_variants() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("4h").unwrap(), Duration::from_secs(14400));
        assert_eq!(parse_duration("1d").unwrap(), Duration::from_secs(86400));
        assert_eq!(parse_duration("1w").unwrap(), Duration::from_secs(604_800));
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(parse_duration("45").unwrap(), Duration::from_secs(45));
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("10x").is_err());
        assert!(parse_duration("10").is_ok());
        assert!(parse_duration("99999999999999999999999999w").is_err());
    }

    #[test]
    fn test_parse_matcher_all_operators() {
        let eq = parse_matcher_str(r#"severity = "critical""#).unwrap();
        assert_eq!(eq.label, "severity");
        assert_eq!(eq.op, MatchOp::Eq);
        assert_eq!(eq.value, "critical");

        let ne = parse_matcher_str("env!=dev").unwrap();
        assert_eq!(ne.op, MatchOp::Ne);
        assert_eq!(ne.value, "dev");

        let re = parse_matcher_str("service.name =~ 'api-.*'").unwrap();
        assert_eq!(re.label, "service.name");
        assert_eq!(re.op, MatchOp::Regex);
        assert_eq!(re.value, "api-.*");

        let nre = parse_matcher_str("team !~ test-.*").unwrap();
        assert_eq!(nre.op, MatchOp::NotRegex);
        assert_eq!(nre.value, "test-.*");

        assert!(parse_matcher_str("").is_err());
        assert!(parse_matcher_str("no_operator").is_err());
        assert!(parse_matcher_str("=val").is_err());
        assert!(parse_matcher_str("service =~ [invalid").is_err());
    }

    #[test]
    fn test_resolve_secret_with() {
        let lookup = |k: &str| match k {
            "SLACK_URL" => Ok("https://hooks.slack.com/services/T/B/X".to_string()),
            "PD_KEY" => Ok("pd-secret-123".to_string()),
            _ => Err(std::env::VarError::NotPresent),
        };

        assert_eq!(
            resolve_secret_with("${SLACK_URL}", lookup).unwrap(),
            "https://hooks.slack.com/services/T/B/X"
        );
        assert_eq!(
            resolve_secret_with("prefix-{env:PD_KEY}-suffix", lookup).unwrap(),
            "prefix-pd-secret-123-suffix"
        );
        assert_eq!(resolve_secret_with("literal", lookup).unwrap(), "literal");
        assert!(resolve_secret_with("${MISSING}", lookup).is_err());
        assert!(resolve_secret_with("{env:MISSING}", lookup).is_err());
        assert!(resolve_secret_with("${UNCLOSED", lookup).is_err());
        assert!(resolve_secret_with("{env:UNCLOSED", lookup).is_err());
        assert!(resolve_secret_with("${}", lookup).is_err());
        assert!(resolve_secret_with("{env:}", lookup).is_err());
        // Exercise resolve_secret wrapper against missing var
        assert!(resolve_secret("${__PARQTEL_NONEXISTENT_TEST_VAR__}").is_err());
    }

    #[test]
    fn test_validation_and_redaction() {
        let mut cfg = AlertmanagerRoutingConfig {
            receivers: vec![ReceiverConfig {
                name: "slack-ops".into(),
                slack_configs: vec![SlackReceiverConfig {
                    webhook_url: "https://hooks.slack.com/secret".into(),
                    ..Default::default()
                }],
                pagerduty_configs: vec![PagerDutyReceiverConfig {
                    routing_key: "pd-key".into(),
                    ..Default::default()
                }],
                teams_configs: vec![TeamsReceiverConfig {
                    webhook_url: "https://outlook.office.com/webhook/secret".into(),
                    ..Default::default()
                }],
                webhook_configs: vec![WebhookReceiverConfig {
                    url: "https://example.com/hook".into(),
                    headers: BTreeMap::from([("Authorization".into(), "Bearer tok".into())]),
                    ..Default::default()
                }],
            }],
            route: Some(AlertRouteConfig {
                receiver: Some("slack-ops".into()),
                matchers: vec!["severity =~ critical|warning".into()],
                group_wait: Some("10s".into()),
                routes: vec![AlertRouteConfig {
                    matchers: vec!["team = platform".into()],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            inhibit_rules: vec![InhibitRuleConfig {
                source_matchers: vec!["severity = critical".into()],
                target_matchers: vec!["severity = warning".into()],
                equal: vec!["service.name".into()],
            }],
            ..Default::default()
        };

        let mut errors = Vec::new();
        cfg.validate_into("alerting.routing", &mut errors);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");

        let red = cfg.redacted();
        assert_eq!(red.receivers[0].slack_configs[0].webhook_url, REDACTED);
        assert_eq!(red.receivers[0].pagerduty_configs[0].routing_key, REDACTED);
        assert_eq!(red.receivers[0].teams_configs[0].webhook_url, REDACTED);
        assert_eq!(red.receivers[0].webhook_configs[0].url, REDACTED);
        assert_eq!(
            red.receivers[0].webhook_configs[0].headers["Authorization"],
            REDACTED
        );

        // Dangling receiver + bad regex + bad duration + max_active_groups=0
        cfg.max_active_groups = 0;
        cfg.global.http_timeout = "bad".into();
        cfg.route.as_mut().unwrap().receiver = Some("missing".into());
        cfg.inhibit_rules[0]
            .source_matchers
            .push("x =~ [unclosed".into());
        let mut errs = Vec::new();
        cfg.validate_into("alerting.routing", &mut errs);
        assert!(errs.len() >= 4);
    }
}
