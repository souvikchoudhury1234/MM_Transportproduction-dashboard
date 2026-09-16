"""
Fetch yesterday's per-agent production numbers from Grafana and merge into data.json.

Environment variables:
  GRAFANA_TOKEN  - service account token (starts with glsa_)
  GRAFANA_URL    - default: https://grafana.100ms.ai
  REPORT_DATE    - optional YYYY-MM-DD (defaults to yesterday in US-Eastern)

Idempotent: rerunning for the same date overwrites that day's rows.
"""
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - py<3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore

import requests

DATASOURCE_UID = "afpll1qs9sb28d"
DATASOURCE_TYPE = "grafana-clickhouse-datasource"
DATA_FILE = Path(__file__).parent / "data.json"

# Team assignments (name -> "MM" or "Transport"). Extend as new agents appear.
TEAM_MAP = {
    # Transport team members go here — everything else defaults to MM.
    # Example: "John Doe": "Transport",
}

MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November", "December"]
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


SQL_TEMPLATE = """WITH completed_cases AS (SELECT DISTINCT case_record_id FROM compass.timeline_events WHERE event_type = 'case.state.changed' AND to_state = 'completed' AND organization_id = '68a0732edbe070ddd4e5ca2c' AND program_id IN ('6a87160b5a58ce10be2c3af6','6a6cbae09ff9773b3742665d','6a67ab377f5f92259997305f','6a327b8be85586c6adb34c16','697348961ab5e97a33f32776','69d8ca744086b73c2e2a7dcd','69d8c733fd8cf216b0b56c94','69d8c9ae4086b73c2e2a7c0a','6a0ae34eb3b5d12ed4aaa702','69e61717ce4fd8fdd3caa302','69e60211d2e5ee78caea818c','69fb13602468dbeda285ff32','6aa1bcee0160bbb51cf4649b','6a5a1055d118cc325bac59e2','69e1d618d2e5ee78cadee2fc','69d8cb434086b73c2e2a80b2','68a087f2def9b0fa08d27b2b','69fcaf190d966c42c8214fad','6a5a108857b2ec70a12c12c2','698379f965c5641fba870c29') AND workflow_type = 'unifiedmedbv' AND toDate(addHours(created_at, -4)) = toDate('{report_date}')), cases_with_human AS (SELECT DISTINCT case_record_id FROM compass.timeline_events WHERE organization_id = '68a0732edbe070ddd4e5ca2c' AND case_record_id IN (SELECT case_record_id FROM completed_cases) AND event_type = 'case.state.changed' AND (to_state = 'human_audit' OR to_state = 'human_call')), assigned_via_event AS (SELECT case_record_id, argMax(assigned_to_user_id, created_at) as user_id FROM compass.timeline_events WHERE event_type = 'case.assigned' AND organization_id = '68a0732edbe070ddd4e5ca2c' AND assigned_to_user_id != '' AND case_record_id IN (SELECT case_record_id FROM cases_with_human) GROUP BY case_record_id), fallback_human_call AS (SELECT case_record_id, argMin(actor_id, created_at) as user_id FROM compass.timeline_events WHERE case_record_id IN (SELECT case_record_id FROM cases_with_human) AND case_record_id NOT IN (SELECT case_record_id FROM assigned_via_event) AND actor_id != '' AND event_type = 'case.call.started' AND call_type = 'human_call' GROUP BY case_record_id), fallback_audit_complete AS (SELECT case_record_id, argMax(actor_id, created_at) as user_id FROM compass.timeline_events WHERE case_record_id IN (SELECT case_record_id FROM cases_with_human) AND case_record_id NOT IN (SELECT case_record_id FROM assigned_via_event) AND case_record_id NOT IN (SELECT case_record_id FROM fallback_human_call) AND actor_id != '' AND event_type = 'case.state.changed' AND from_state = 'human_audit' AND to_state = 'completed' GROUP BY case_record_id), all_assigned AS (SELECT case_record_id, user_id FROM assigned_via_event UNION ALL SELECT case_record_id, user_id FROM fallback_human_call UNION ALL SELECT case_record_id, user_id FROM fallback_audit_complete), user_cases AS (SELECT user_id as actor_id, count(DISTINCT case_record_id) as cases_completed FROM all_assigned GROUP BY user_id), deduped_calls AS (SELECT actor_id, call_id, any(call_duration) as duration FROM compass.timeline_events WHERE event_type = 'case.call.completed' AND organization_id = '68a0732edbe070ddd4e5ca2c' AND program_id IN ('6a87160b5a58ce10be2c3af6','6a6cbae09ff9773b3742665d','6a67ab377f5f92259997305f','6a327b8be85586c6adb34c16','697348961ab5e97a33f32776','69d8ca744086b73c2e2a7dcd','69d8c733fd8cf216b0b56c94','69d8c9ae4086b73c2e2a7c0a','6a0ae34eb3b5d12ed4aaa702','69e61717ce4fd8fdd3caa302','69e60211d2e5ee78caea818c','69fb13602468dbeda285ff32','6aa1bcee0160bbb51cf4649b','6a5a1055d118cc325bac59e2','69e1d618d2e5ee78cadee2fc','69d8cb434086b73c2e2a80b2','68a087f2def9b0fa08d27b2b','69fcaf190d966c42c8214fad','6a5a108857b2ec70a12c12c2','698379f965c5641fba870c29') AND workflow_type = 'unifiedmedbv' AND call_type = 'human_call' AND actor_id != '' AND toDate(addHours(created_at, -4)) = toDate('{report_date}') GROUP BY actor_id, call_id), user_talk AS (SELECT actor_id, sum(duration) as total_duration FROM deduped_calls GROUP BY actor_id), user_evals AS (SELECT a.user_id as actor_id, countIf(e.compliance_failed = true) as compliance_fails, countIf(e.needs_review = true) as reviews FROM all_assigned a INNER JOIN analytics.evaluation_events e ON a.case_record_id = e.case_record_id AND e.organization_id = '68a0732edbe070ddd4e5ca2c' AND e.status = 'completed' GROUP BY a.user_id) SELECT any(JSON_VALUE(toString(u.doc), '$.name')) as user_name, any(JSON_VALUE(toString(u.doc), '$.email')) as email, uc.cases_completed, coalesce(ut.total_duration, 0) as talk_time, coalesce(ue.compliance_fails, 0) as compliance_fails, coalesce(ue.reviews, 0) as needs_review FROM user_cases uc INNER JOIN compass.compass_users u ON uc.actor_id = u._id LEFT JOIN user_talk ut ON uc.actor_id = ut.actor_id LEFT JOIN user_evals ue ON uc.actor_id = ue.actor_id GROUP BY uc.actor_id, uc.cases_completed, talk_time, compliance_fails, needs_review ORDER BY uc.cases_completed DESC"""


def yesterday_us_eastern() -> str:
    return (datetime.now(ZoneInfo("America/New_York")) - timedelta(days=1)).strftime("%Y-%m-%d")


def fetch_from_grafana(base_url: str, token: str, report_date: str):
    """Return list of dicts: user_name, email, cases_completed, talk_time, compliance_fails, needs_review."""
    sql = SQL_TEMPLATE.format(report_date=report_date)
    body = {
        "queries": [{
            "refId": "A",
            "datasource": {"uid": DATASOURCE_UID, "type": DATASOURCE_TYPE},
            "rawSql": sql,
            "format": 1,
            "queryType": "table",
            "meta": {"timezone": "America/New_York"},
        }],
        "from": "now-1d",
        "to": "now",
    }
    r = requests.post(
        f"{base_url.rstrip('/')}/api/ds/query",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body,
        timeout=60,
    )
    r.raise_for_status()
    payload = r.json()
    try:
        frame = payload["results"]["A"]["frames"][0]
    except (KeyError, IndexError):
        raise RuntimeError(f"Unexpected Grafana response: {json.dumps(payload)[:500]}")
    field_names = [f["name"] for f in frame["schema"]["fields"]]
    columns = frame["data"]["values"]
    rows = []
    for i in range(len(columns[0]) if columns else 0):
        rows.append({name: columns[idx][i] for idx, name in enumerate(field_names)})
    return rows


def parse_talk_time(v) -> int:
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).strip()
    if ":" in s:
        parts = s.split(":")
        try:
            parts = [int(p) for p in parts]
        except ValueError:
            return 0
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        if len(parts) == 2:
            return parts[0] * 60 + parts[1]
    try:
        return int(float(s))
    except ValueError:
        return 0


def load_data():
    if DATA_FILE.exists():
        return json.loads(DATA_FILE.read_text())
    return {}


def save_data(data):
    DATA_FILE.write_text(json.dumps(data, indent=None, separators=(",", ":")))


def ensure_month(data, month_key, year, month):
    """Make sure the month exists in data with a days array covering the full month."""
    if month_key in data:
        return
    # Days in month
    if month == 12:
        next_month_first = datetime(year + 1, 1, 1)
    else:
        next_month_first = datetime(year, month + 1, 1)
    last_day = (next_month_first - timedelta(days=1)).day
    days = []
    for d in range(1, last_day + 1):
        wd_idx = datetime(year, month, d).weekday()
        days.append({"d": d, "wd": WEEKDAYS[wd_idx]})
    data[month_key] = {"days": days, "employees": []}


def merge(data, rows, report_date: str):
    dt = datetime.strptime(report_date, "%Y-%m-%d")
    month_key = f"{MONTH_NAMES[dt.month - 1]} {dt.year}"
    day = dt.day
    ensure_month(data, month_key, dt.year, dt.month)
    emps = data[month_key]["employees"]
    # index by name for quick lookup
    by_name = {e["name"]: e for e in emps}
    for row in rows:
        name = (row.get("user_name") or "").strip()
        if not name:
            continue
        cases = int(row.get("cases_completed") or 0)
        talk = parse_talk_time(row.get("talk_time"))
        fails = int(row.get("compliance_fails") or 0)
        review = int(row.get("needs_review") or 0)
        team = TEAM_MAP.get(name, "MM")
        emp = by_name.get(name)
        if emp is None:
            emp = {"name": name, "team": team, "days": {}}
            emps.append(emp)
            by_name[name] = emp
        emp["days"][str(day)] = {
            "s": "W",
            "v": cases,
            "tt": talk,
            "f": fails,
            "r": review,
        }


def main():
    token = os.environ.get("GRAFANA_TOKEN")
    if not token:
        print("ERROR: GRAFANA_TOKEN environment variable is required.", file=sys.stderr)
        sys.exit(1)
    base_url = os.environ.get("GRAFANA_URL", "https://grafana.100ms.ai")
    report_date = os.environ.get("REPORT_DATE") or yesterday_us_eastern()
    print(f"Fetching {report_date} from {base_url} ...")
    rows = fetch_from_grafana(base_url, token, report_date)
    print(f"Received {len(rows)} agent rows.")
    data = load_data()
    merge(data, rows, report_date)
    save_data(data)
    print(f"Wrote {DATA_FILE}")


if __name__ == "__main__":
    main()
