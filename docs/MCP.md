# MCP (Model Context Protocol) Integrations

Parqtel ships with 7 MCP servers that expose tools for LLM-driven incident response. Each server is a standalone binary built on the shared `parqtel-mcp-core` framework.

## Overview

The Model Context Protocol enables AI agents (Claude, GPT, etc.) to interact with external services through a standardized JSON-RPC interface. Parqtel's MCP servers allow an LLM to:

- Post alerts and RCA updates to communication channels
- Create and manage incidents in on-call systems
- Generate postmortem documents automatically
- Query observability data for root cause analysis

## Architecture

```
┌──────────────────┐     JSON-RPC      ┌──────────────────┐
│   LLM / Agent    │ ◄──────────────── │  MCP Server      │
│  (Claude, GPT)   │ ──────────────── ►│  (parqtel-mcp-*) │
└──────────────────┘                    └────────┬─────────┘
                                                 │ HTTP/REST
                                                 ▼
                                        ┌──────────────────┐
                                        │ External Service │
                                        │ (Slack, PD, etc.)│
                                        └──────────────────┘
```

Each MCP server:
- Exposes `/health` for liveness probes
- Implements rate limiting (token bucket, configurable per-minute)
- Registers tools with typed JSON Schema inputs
- Sanitizes parameters before forwarding to external APIs

## Shared Configuration

All MCP servers accept these environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `MCP_HOST` | Bind address | `0.0.0.0` |
| `MCP_PORT` | Listen port | `3000` |
| `MCP_RATE_LIMIT` | Requests per minute | `60` |

## Servers

---

### parqtel-mcp-slack (Port 3001)

Post alerts, RCA updates, resolution notifications, and create incident channels in Slack.

**Environment Variables:**

| Variable | Required | Description |
|----------|----------|-------------|
| `SLACK_BOT_TOKEN` | Yes | Slack Bot OAuth token (`xoxb-...`) |

**Tools:**

#### `send_alert_message`
Post a formatted incident alert to a Slack channel.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `channel` | string | ✓ | Channel to post to |
| `severity` | string | ✓ | Alert severity level |
| `title` | string | ✓ | Alert title |
| `summary` | string | ✓ | Alert summary |
| `runbook_url` | string | | Optional runbook URL |
| `alert_id` | string | ✓ | Unique alert identifier |

#### `send_rca_update`
Post a root cause analysis update as a thread reply.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `channel` | string | ✓ | Channel ID |
| `thread_ts` | string | ✓ | Thread timestamp |
| `primary_cause` | string | ✓ | Root cause description |
| `confidence` | number | ✓ | Confidence score (0-1) |
| `evidence_summary` | string | ✓ | Evidence summary |
| `recommended_actions` | string[] | ✓ | List of recommended actions |

#### `resolve_notification`
Post a resolution notification to the original thread.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `channel` | string | ✓ | Channel ID |
| `thread_ts` | string | ✓ | Thread timestamp |
| `resolution_summary` | string | ✓ | Resolution description |
| `duration_minutes` | number | ✓ | Incident duration |

#### `create_incident_channel`
Create a dedicated incident channel for major incidents.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `incident_id` | string | ✓ | Incident identifier |
| `severity` | string | ✓ | Severity level |
| `affected_services` | string[] | ✓ | List of affected services |

---

### parqtel-mcp-pagerduty (Port 3002)

Create incidents, add notes, resolve incidents, and query on-call schedules.

**Environment Variables:**

| Variable | Required | Description |
|----------|----------|-------------|
| `PAGERDUTY_API_KEY` | Yes | PagerDuty REST API key |

**Tools:**

#### `create_incident`
Create a PagerDuty incident.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `title` | string | ✓ | Incident title |
| `severity` | string | ✓ | Severity level |
| `body` | string | ✓ | Incident body/description |
| `service_id` | string | ✓ | PagerDuty service ID |
| `alert_id` | string | ✓ | Parqtel alert ID |
| `routing_key` | string | | Optional routing key |

#### `add_note`
Add a note to an existing incident.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `incident_id` | string | ✓ | PagerDuty incident ID |
| `note` | string | ✓ | Note content |

#### `resolve_incident`
Resolve a PagerDuty incident.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `incident_id` | string | ✓ | PagerDuty incident ID |
| `resolution_note` | string | ✓ | Resolution description |

#### `get_oncall`
Return the current on-call engineer for a service.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `service_id` | string | ✓ | PagerDuty service ID |

#### `get_recent_incidents`
Return incidents for a service in the last N hours.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `service_id` | string | ✓ | PagerDuty service ID |
| `hours` | number | ✓ | Lookback window in hours |

---

### parqtel-mcp-jira (Port 3003)

Create incident issues, add RCA comments, create action items, and transition issues.

**Environment Variables:**

| Variable | Required | Description |
|----------|----------|-------------|
| `JIRA_BASE_URL` | Yes | Jira instance URL |
| `JIRA_USER_EMAIL` | Yes | Jira user email |
| `JIRA_API_TOKEN` | Yes | Jira API token |

**Tools:**

#### `create_incident_issue`
Create a Jira issue for an incident.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `project_key` | string | ✓ | Jira project key |
| `summary` | string | ✓ | Issue summary |
| `description` | string | ✓ | Issue description |
| `priority` | string | ✓ | Priority level |
| `labels` | string[] | ✓ | Issue labels |
| `alert_id` | string | ✓ | Parqtel alert ID |

#### `add_rca_comment`
Add RCA findings as a formatted Jira comment.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `issue_key` | string | ✓ | Jira issue key (e.g., `OPS-123`) |
| `root_cause` | string | ✓ | Root cause description |
| `evidence` | string[] | ✓ | Evidence items |
| `recommended_actions` | string[] | ✓ | Recommended actions |

#### `create_action_item`
Create a child issue for a postmortem action item.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `parent_issue_key` | string | ✓ | Parent issue key |
| `summary` | string | ✓ | Action item summary |
| `assignee_email` | string | | Assignee email |
| `due_date` | string | | Due date (ISO 8601) |

#### `transition_issue`
Move an issue to a new status.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `issue_key` | string | ✓ | Jira issue key |
| `transition_name` | string | ✓ | Target status name |

---

### parqtel-mcp-notion (Port 3004)

Create incident pages, update status, append RCA sections, and generate postmortem documents.

**Environment Variables:**

| Variable | Required | Description |
|----------|----------|-------------|
| `NOTION_API_KEY` | Yes | Notion integration token (`secret_...`) |

**Tools:**

#### `create_incident_page`
Create an incident page from a template.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `database_id` | string | ✓ | Notion database ID |
| `title` | string | ✓ | Page title |
| `severity` | string | ✓ | Severity level |
| `affected_services` | string[] | ✓ | Affected services |
| `alert_id` | string | ✓ | Parqtel alert ID |
| `started_at` | string | ✓ | Incident start time |

#### `update_incident_status`
Update the status property on an incident page.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `page_id` | string | ✓ | Notion page ID |
| `status` | string | ✓ | New status value |

#### `append_rca_section`
Append a root cause analysis section to an existing page.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `page_id` | string | ✓ | Notion page ID |
| `root_cause` | string | ✓ | Root cause description |
| `timeline` | object[] | ✓ | Timeline entries |
| `recommended_actions` | string[] | ✓ | Recommended actions |

#### `create_postmortem_page`
Create a full postmortem document from AI-drafted content.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `database_id` | string | ✓ | Notion database ID |
| `incident_title` | string | ✓ | Incident title |
| `postmortem_markdown` | string | ✓ | Postmortem content (markdown) |
| `action_items` | object[] | ✓ | Action items |

---

### parqtel-mcp-discord (Port 3005)

Post alert embeds, RCA updates, and resolution notifications to Discord.

**Environment Variables:**

| Variable | Required | Description |
|----------|----------|-------------|
| `DISCORD_BOT_TOKEN` | Yes | Discord bot token |

**Tools:**

#### `send_alert_embed`
Post an embed message to a Discord channel.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `channel_id` | string | ✓ | Discord channel ID |
| `title` | string | ✓ | Embed title |
| `description` | string | ✓ | Embed description |
| `severity` | string | ✓ | Severity level |
| `fields` | object[] | | Additional embed fields |
| `alert_id` | string | ✓ | Parqtel alert ID |

#### `send_rca_update`
Post an RCA update to a thread.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `thread_id` | string | ✓ | Discord thread ID |
| `root_cause` | string | ✓ | Root cause description |
| `confidence` | number | ✓ | Confidence score (0-1) |
| `actions` | string[] | ✓ | Recommended actions |

#### `resolve_alert`
Update the original embed with resolved status.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `message_id` | string | ✓ | Original message ID |
| `channel_id` | string | ✓ | Channel ID |
| `resolution_summary` | string | ✓ | Resolution description |

---

### parqtel-mcp-gdocs (Port 3006)

Create postmortem documents, append timelines, and share documents via Google Docs.

**Environment Variables:**

| Variable | Required | Description |
|----------|----------|-------------|
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Yes | Service account credentials JSON |

**Tools:**

#### `create_postmortem_doc`
Create a Google Doc from the postmortem template.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `folder_id` | string | ✓ | Google Drive folder ID |
| `title` | string | ✓ | Document title |
| `content_markdown` | string | ✓ | Postmortem content (markdown) |
| `action_items` | object[] | ✓ | Action items |

#### `append_timeline`
Append a timeline section to an existing doc.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `doc_id` | string | ✓ | Google Doc ID |
| `timeline` | object[] | ✓ | Timeline entries |

#### `share_document`
Share the document with specific emails.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `doc_id` | string | ✓ | Google Doc ID |
| `emails` | string[] | ✓ | Email addresses |
| `role` | string | ✓ | Permission role (reader, writer, commenter) |

---

### parqtel-mcp-parqtel (Port 3007)

Query Parqtel's own metrics, logs, alerts, and topology data for AI-driven analysis.

**Environment Variables:**

| Variable | Required | Description |
|----------|----------|-------------|
| `PARQTEL_API_URL` | Yes | Parqtel server URL (e.g., `http://parqtel:9090`) |

**Tools:**

#### `query_metrics`
Execute a Prometheus range query.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | ✓ | PromQL expression |
| `start_ns` | number | ✓ | Start time (nanoseconds) |
| `end_ns` | number | ✓ | End time (nanoseconds) |
| `step_secs` | number | ✓ | Step interval (seconds) |

#### `query_logs`
Query log records.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `filter` | string | ✓ | Log filter expression |
| `start_ns` | number | ✓ | Start time (nanoseconds) |
| `end_ns` | number | ✓ | End time (nanoseconds) |
| `limit` | number | ✓ | Maximum results |
| `severity_min` | string | | Minimum severity filter |

#### `get_alert_history`
Return alert history for a service or pod.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `service_name` | string | | Service name filter |
| `pod_name` | string | | Pod name filter |
| `since_hours` | number | | Lookback window in hours |

#### `get_topology`
Return Kubernetes topology for a namespace.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `namespace` | string | ✓ | Kubernetes namespace |

#### `get_noise_statistics`
Return noise scoring statistics for rules.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `rule_id` | string | | Specific rule ID (omit for all) |

## Running MCP Servers

### Standalone

```bash
# Build all MCP servers
cargo build --release -p parqtel-mcp-slack -p parqtel-mcp-pagerduty \
  -p parqtel-mcp-jira -p parqtel-mcp-notion -p parqtel-mcp-discord \
  -p parqtel-mcp-gdocs -p parqtel-mcp-parqtel

# Run one
SLACK_BOT_TOKEN=xoxb-... MCP_PORT=3001 ./target/release/parqtel-mcp-slack
```

### Docker Compose

All MCP servers are included in the Docker Compose stack (commented out by default). Uncomment the ones you need in `docker-compose.yml`, then start them:

```bash
# From project root
docker compose up -d mcp-slack mcp-pagerduty mcp-jira
```

### Kubernetes (Helm)

Enable MCP servers in your Helm values:

```yaml
mcp:
  slack:
    enabled: true
    env:
      SLACK_BOT_TOKEN: "xoxb-..."
  pagerduty:
    enabled: true
    env:
      PAGERDUTY_API_KEY: "..."
```

## Incident Response Workflow

A typical AI-driven incident response flow:

1. **Alert fires** → Parqtel evaluates rule, transitions to `Firing`
2. **Notify** → LLM calls `send_alert_message` (Slack) + `create_incident` (PagerDuty)
3. **Investigate** → LLM calls `query_metrics` + `query_logs` (Parqtel MCP)
4. **RCA** → LLM calls `send_rca_update` (Slack) + `add_rca_comment` (Jira)
5. **Resolve** → LLM calls `resolve_incident` (PagerDuty) + `resolve_notification` (Slack)
6. **Postmortem** → LLM calls `create_postmortem_doc` (Google Docs) or `create_postmortem_page` (Notion)
