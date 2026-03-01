#!/usr/bin/env python3
"""
onboard_entity.py  --entity <name>  [options]

End-to-end onboarding of a new DQ entity in one command:
  1. Generates 00_setup.py from entity config
  2. Uploads shared notebooks (01/02/03) once to DQ_Business_shared/
  3. Uploads entity-specific 00_setup.py to DQ_Business_{entity}/
  4. Creates or updates the Databricks job (DQ_{entity})
  5. Triggers a run and polls to completion
  6. Deploys and publishes the AI/BI dashboard

Usage:
    python onboard_entity.py --entity fund_trans_gold
    python onboard_entity.py --entity loan_gold --no-run --no-dashboard
    python onboard_entity.py --entity customer_gold --force-upload
"""

import argparse
import base64
import importlib.util
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# ── paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR       = Path(__file__).parent
SHARED_NB_DIR    = SCRIPT_DIR / "shared_notebooks"
ENTITY_CFG_DIR   = SCRIPT_DIR / "entity_configs"
WORKSPACE_USER   = "Users/andre.r23@gmx.net"
SHARED_WS_PATH   = f"/{WORKSPACE_USER}/DQ_Business_shared"
WH_ID            = "94cbf5bc4ec73e30"   # serverless_x_small

# Default catalog/schema — overridable via CLI args
DEFAULT_CATALOG  = "databricks_snippets_7405610928938750"
DEFAULT_SCHEMA   = "dbdemos_dq_business_arausch"


# ── Databricks REST helpers ────────────────────────────────────────────────────

def _load_credentials():
    import configparser
    cfg = configparser.ConfigParser()
    cfg.read(os.path.expanduser("~/.databrickscfg"))
    section = cfg.sections()[0] if cfg.sections() else "DEFAULT"
    host  = cfg[section].get("host", "").rstrip("/")
    token = cfg[section].get("token", "")
    if not host or not token:
        sys.exit("ERROR: Could not read host/token from ~/.databrickscfg")
    return host, token


def _db(host, token, method, path, body=None, params=None):
    url = f"{host}/api/2.0/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(
        url, data=data, method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        msg = e.read().decode()
        print(f"  HTTP {e.code} {method} {path}: {msg[:300]}")
        raise


# ── notebook upload ────────────────────────────────────────────────────────────

def _notebook_exists(host, token, ws_path):
    try:
        _db(host, token, "GET", "workspace/get-status", params={"path": ws_path})
        return True
    except urllib.error.HTTPError:
        return False


def _upload_notebook(host, token, local_path, ws_path, force=False):
    if not force and _notebook_exists(host, token, ws_path):
        print(f"  [skip] {ws_path} already exists")
        return
    with open(local_path, "rb") as f:
        content_b64 = base64.b64encode(f.read()).decode()
    _db(host, token, "POST", "workspace/import", {
        "path": ws_path, "format": "SOURCE", "language": "PYTHON",
        "content": content_b64, "overwrite": True
    })
    print(f"  [ok]   {ws_path}")


def upload_shared_notebooks(host, token, force=False):
    print("\n-- Shared notebooks --")
    _db(host, token, "POST", "workspace/mkdirs", {"path": SHARED_WS_PATH})
    for nb_file in ["01_apply_dq_rules.py", "02_aggregate_dq_results.py", "03_dq_dashboard.py"]:
        local = SHARED_NB_DIR / nb_file
        nb_name = nb_file.replace(".py", "")
        _upload_notebook(host, token, local, f"{SHARED_WS_PATH}/{nb_name}", force=force)


def upload_entity_setup(host, token, entity, setup_source, force=False):
    ws_dir  = f"/{WORKSPACE_USER}/DQ_Business_{entity}"
    ws_path = f"{ws_dir}/00_setup"
    print(f"\n-- Entity setup notebook ({entity}) --")
    _db(host, token, "POST", "workspace/mkdirs", {"path": ws_dir})
    content_b64 = base64.b64encode(setup_source.encode()).decode()
    _db(host, token, "POST", "workspace/import", {
        "path": ws_path, "format": "SOURCE", "language": "PYTHON",
        "content": content_b64, "overwrite": True
    })
    print(f"  [ok]   {ws_path}")
    return ws_path


# ── job creation / update ──────────────────────────────────────────────────────

def create_or_update_job(host, token, entity, config, catalog, dq_schema):
    entity_ws_dir = f"/{WORKSPACE_USER}/DQ_Business_{entity}"
    job_name      = f"DQ_{entity}"

    common_params = {"catalog": catalog, "dq_schema": dq_schema, "target_table": entity}

    tasks = [
        {
            "task_key": "setup",
            "notebook_task": {
                "notebook_path": f"{entity_ws_dir}/00_setup",
                "base_parameters": {
                    **common_params,
                    "source_catalog": config.SOURCE_CATALOG,
                    "source_schema":  config.SOURCE_SCHEMA,
                }
            }
        },
        {
            "task_key": "apply_dq_rules",
            "depends_on": [{"task_key": "setup"}],
            "notebook_task": {
                "notebook_path": f"{SHARED_WS_PATH}/01_apply_dq_rules",
                "base_parameters": common_params
            }
        },
        {
            "task_key": "aggregate_dq_results",
            "depends_on": [{"task_key": "apply_dq_rules"}],
            "notebook_task": {
                "notebook_path": f"{SHARED_WS_PATH}/02_aggregate_dq_results",
                "base_parameters": common_params
            }
        },
        {
            "task_key": "dq_dashboard",
            "depends_on": [{"task_key": "aggregate_dq_results"}],
            "notebook_task": {
                "notebook_path": f"{SHARED_WS_PATH}/03_dq_dashboard",
                "base_parameters": {"catalog": catalog, "dq_schema": dq_schema, "target_table": entity}
            }
        },
    ]

    job_payload = {
        "name": job_name,
        "tasks": tasks,
        "queue": {"enabled": True},
    }

    if getattr(config, "SCHEDULE_CRON", None):
        job_payload["schedule"] = {
            "quartz_cron_expression": config.SCHEDULE_CRON,
            "timezone_id": "UTC",
            "pause_status": "UNPAUSED",
        }

    # Find existing job by name
    existing = _db(host, token, "GET", "jobs/list")
    job_id   = None
    for j in existing.get("jobs", []):
        if j.get("settings", {}).get("name") == job_name:
            job_id = j["job_id"]
            break

    if job_id:
        print(f"\n-- Job: updating existing id={job_id} ({job_name}) --")
        _db(host, token, "POST", "jobs/reset", {"job_id": job_id, "new_settings": job_payload})
    else:
        print(f"\n-- Job: creating new ({job_name}) --")
        result = _db(host, token, "POST", "jobs/create", job_payload)
        job_id = result["job_id"]

    print(f"  Job ID: {job_id}")
    return job_id


# ── run + poll ─────────────────────────────────────────────────────────────────

def trigger_and_wait(host, token, job_id):
    print(f"\n-- Triggering job {job_id} --")
    run = _db(host, token, "POST", "jobs/run-now", {"job_id": job_id})
    run_id = run["run_id"]
    print(f"  run_id = {run_id}")

    while True:
        r       = _db(host, token, "GET", f"jobs/runs/get?run_id={run_id}")
        state   = r["state"]["life_cycle_state"]
        result  = r["state"].get("result_state", "")
        tasks   = [(t["task_key"], t["state"]["life_cycle_state"],
                    t["state"].get("result_state", ""))
                   for t in r.get("tasks", [])]
        running = [t[0] for t in tasks if t[1] == "RUNNING"]
        done    = [(t[0], t[2]) for t in tasks if t[1] == "TERMINATED"]
        print(f"  {state} | running={running} | done={done}")
        if state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        time.sleep(20)

    if result != "SUCCESS":
        print(f"\n  [ERROR] Run finished with: {state}/{result}")
        sys.exit(1)

    print(f"\n  Run completed: {state}/{result}")
    return run_id


# ── dashboard ──────────────────────────────────────────────────────────────────

def build_dashboard_json(entity, catalog, schema):
    """Build AI/BI dashboard JSON scoped to this entity."""
    C, S = catalog, schema

    latest_ts = (
        f"(SELECT MAX(r2.dq_execution_timestamp) "
        f"FROM {C}.{S}.dq_results r2 "
        f"JOIN {C}.{S}.dq_rule_mappings m2 ON r2.dq_rule_no = m2.dq_rule_no "
        f"WHERE m2.source_table = '{entity}')"
    )
    ef = f"AND m.source_table = '{entity}' AND r.dq_execution_timestamp = {latest_ts}"

    datasets = [
        {
            "name": "ds_summary", "displayName": "Overall Summary",
            "queryLines": [(
                f"SELECT COUNT(DISTINCT r.dq_rule_no) AS TOTAL_RULES, "
                f"COUNT(DISTINCT CASE WHEN r.dq_number_relevant_records > 0 THEN r.dq_rule_no END) AS RULES_APPLIED, "
                f"COUNT(DISTINCT CASE WHEN r.dq_number_relevant_records = 0 THEN r.dq_rule_no END) AS RULES_SKIPPED, "
                f"SUM(r.dq_number_relevant_records) AS TOTAL_CHECKS, "
                f"SUM(r.dq_number_pos_results) AS TOTAL_PASSED, "
                f"SUM(r.dq_number_relevant_records - r.dq_number_pos_results) AS TOTAL_FAILED, "
                f"ROUND(SUM(r.dq_number_pos_results)*1.0/NULLIF(SUM(r.dq_number_relevant_records),0)*100,2) AS OVERALL_PASS_PCT, "
                f"DATE_FORMAT(MAX(r.dq_execution_timestamp),'yyyy-MM-dd HH:mm') AS LAST_RUN "
                f"FROM {C}.{S}.dq_results r "
                f"JOIN {C}.{S}.dq_rule_mappings m ON r.dq_rule_no = m.dq_rule_no "
                f"WHERE m.source_table = '{entity}' AND r.dq_execution_timestamp = {latest_ts}"
            )]
        },
        {
            "name": "ds_by_category", "displayName": "By Category",
            "queryLines": [(
                f"SELECT COALESCE(m.dq_category, g.dq_category) AS DQ_CATEGORY, "
                f"COUNT(DISTINCT r.dq_rule_no) AS TOTAL_RULES, "
                f"SUM(r.dq_number_relevant_records) AS TOTAL_CHECKS, "
                f"SUM(r.dq_number_pos_results) AS TOTAL_PASSED, "
                f"SUM(r.dq_number_relevant_records - r.dq_number_pos_results) AS TOTAL_FAILED, "
                f"ROUND(SUM(r.dq_number_pos_results)*1.0/NULLIF(SUM(r.dq_number_relevant_records),0)*100,2) AS PASS_PCT "
                f"FROM {C}.{S}.dq_results r "
                f"JOIN {C}.{S}.dq_rule_mappings m ON r.dq_rule_no = m.dq_rule_no "
                f"JOIN {C}.{S}.dq_rules_generic g ON m.generic_rule_desc_short = g.generic_rule_desc_short "
                f"WHERE m.source_table = '{entity}' AND r.dq_execution_timestamp = {latest_ts} "
                f"GROUP BY COALESCE(m.dq_category, g.dq_category) ORDER BY DQ_CATEGORY"
            )]
        },
        {
            "name": "ds_rule_detail", "displayName": "Per-Rule Detail",
            "queryLines": [(
                f"SELECT r.dq_rule_no AS DQ_RULE_NO, COALESCE(m.dq_category, g.dq_category) AS DQ_CATEGORY, "
                f"m.source_field AS SOURCE_FIELD, g.generic_rule_desc_short AS RULE_TYPE, "
                f"m.rule_description AS RULE_DESC, r.dq_number_relevant_records AS CHECKS, "
                f"r.dq_number_pos_results AS PASSED, "
                f"r.dq_number_relevant_records - r.dq_number_pos_results AS FAILED, "
                f"ROUND(r.dq_number_pos_results*1.0/NULLIF(r.dq_number_relevant_records,0)*100,2) AS PASS_PCT, "
                f"CASE WHEN r.dq_number_relevant_records = 0 THEN 'SKIPPED' "
                f"     WHEN r.dq_number_pos_results = r.dq_number_relevant_records THEN 'PASS' "
                f"     ELSE 'BREACH' END AS STATUS, "
                f"m.threshold_pct AS THRESHOLD_PCT, m.owner_dept AS OWNER_DEPT, m.owner_user AS OWNER_USER "
                f"FROM {C}.{S}.dq_results r "
                f"JOIN {C}.{S}.dq_rule_mappings m ON r.dq_rule_no = m.dq_rule_no "
                f"JOIN {C}.{S}.dq_rules_generic g ON m.generic_rule_desc_short = g.generic_rule_desc_short "
                f"WHERE m.source_table = '{entity}' AND r.dq_execution_timestamp = {latest_ts} "
                f"ORDER BY DQ_CATEGORY, r.dq_rule_no"
            )]
        },
        {
            "name": "ds_breach_register", "displayName": "Breach Register",
            "queryLines": [(
                f"SELECT r.dq_rule_no AS DQ_RULE_NO, COALESCE(m.dq_category, g.dq_category) AS DQ_CATEGORY, "
                f"m.source_field AS SOURCE_FIELD, m.rule_description AS RULE_DESC, "
                f"r.dq_number_relevant_records AS CHECKS, r.dq_number_pos_results AS PASSED, "
                f"r.dq_number_relevant_records - r.dq_number_pos_results AS FAILED_RECORDS, "
                f"ROUND(r.dq_number_pos_results*1.0/NULLIF(r.dq_number_relevant_records,0)*100,2) AS PASS_PCT, "
                f"m.threshold_pct AS REQUIRED_PCT, m.owner_dept AS OWNER_DEPT, m.owner_user AS OWNER_USER "
                f"FROM {C}.{S}.dq_results r "
                f"JOIN {C}.{S}.dq_rule_mappings m ON r.dq_rule_no = m.dq_rule_no "
                f"JOIN {C}.{S}.dq_rules_generic g ON m.generic_rule_desc_short = g.generic_rule_desc_short "
                f"WHERE m.source_table = '{entity}' AND r.dq_execution_timestamp = {latest_ts} "
                f"AND r.dq_number_relevant_records > 0 "
                f"AND ROUND(r.dq_number_pos_results*1.0/r.dq_number_relevant_records*100,2) < m.threshold_pct "
                f"ORDER BY PASS_PCT ASC"
            )]
        },
    ]

    n_rules = "N"   # placeholder shown in subtitle — dashboard auto-reflects actual count

    def _counter(name, field, title, x, y):
        return {
            "widget": {
                "name": name,
                "queries": [{"name": "main_query", "query": {
                    "datasetName": "ds_summary",
                    "fields": [{"name": field, "expression": f"`{field}`"}],
                    "disaggregated": True
                }}],
                "spec": {
                    "version": 2, "widgetType": "counter",
                    "encodings": {"value": {"fieldName": field, "displayName": title}},
                    "frame": {"showTitle": True, "title": title}
                }
            },
            "position": {"x": x, "y": y, "width": 4, "height": 3}
        }

    def _bar(name, ds, x_field, y_field, x_label, y_label, title, x, y, w=6, h=5):
        return {
            "widget": {
                "name": name,
                "queries": [{"name": "main_query", "query": {
                    "datasetName": ds,
                    "fields": [
                        {"name": x_field, "expression": f"`{x_field}`"},
                        {"name": y_field, "expression": f"`{y_field}`"},
                    ],
                    "disaggregated": True
                }}],
                "spec": {
                    "version": 3, "widgetType": "bar",
                    "encodings": {
                        "x": {"fieldName": x_field, "scale": {"type": "categorical"}, "displayName": x_label},
                        "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_label},
                    },
                    "frame": {"showTitle": True, "title": title}
                }
            },
            "position": {"x": x, "y": y, "width": w, "height": h}
        }

    def _table(name, ds, fields, columns, title, x, y, w=12, h=8):
        return {
            "widget": {
                "name": name,
                "queries": [{"name": "main_query", "query": {
                    "datasetName": ds,
                    "fields": [{"name": f, "expression": f"`{f}`"} for f in fields],
                    "disaggregated": True
                }}],
                "spec": {
                    "version": 2, "widgetType": "table",
                    "encodings": {"columns": [{"fieldName": f, "displayName": d} for f, d in columns]},
                    "frame": {"showTitle": True, "title": title}
                }
            },
            "position": {"x": x, "y": y, "width": w, "height": h}
        }

    def _text(name, md, x, y, w=12, h=1):
        return {
            "widget": {"name": name, "multilineTextboxSpec": {"lines": [md]}},
            "position": {"x": x, "y": y, "width": w, "height": h}
        }

    rule_cols  = ["DQ_RULE_NO","DQ_CATEGORY","SOURCE_FIELD","RULE_DESC","CHECKS","PASSED","FAILED","PASS_PCT","STATUS","THRESHOLD_PCT","OWNER_DEPT","OWNER_USER"]
    rule_hdrs  = ["Rule No","Category","Field","Rule Description","Checks","Passed","Failed","Pass %","Status","Threshold %","Owner Dept","Owner"]
    breach_cols = ["DQ_RULE_NO","DQ_CATEGORY","SOURCE_FIELD","RULE_DESC","CHECKS","FAILED_RECORDS","PASS_PCT","REQUIRED_PCT","OWNER_DEPT","OWNER_USER"]
    breach_hdrs = ["Rule No","Category","Field","Rule Description","Checks","Failed Records","Pass %","Required %","Owner Dept","Owner"]

    layout = [
        _text("title",    f"## DQ Dashboard - {entity}", 0, 0),
        _text("subtitle", "Latest run | DQ Framework",    0, 1, w=8),
        _counter("kpi-last-run",      "LAST_RUN",       "Last Run",       8,  1),
        _counter("kpi-pass-pct",      "OVERALL_PASS_PCT","Overall Pass %",0,  4),
        _counter("kpi-total-failed",  "TOTAL_FAILED",   "Failed Checks",  4,  4),
        _counter("kpi-total-checks",  "TOTAL_CHECKS",   "Total Checks",   8,  4),
        _counter("kpi-total-rules",   "TOTAL_RULES",    "Total Rules",    0,  7),
        _counter("kpi-rules-applied", "RULES_APPLIED",  "Rules Applied",  4,  7),
        _counter("kpi-rules-skipped", "RULES_SKIPPED",  "Rules Skipped",  8,  7),
        _text("section-category", "### Category Breakdown", 0, 10),
        _bar("chart-pass-pct-by-cat",  "ds_by_category", "DQ_CATEGORY", "PASS_PCT",     "Category", "Pass %",         "Pass % by Category",        0, 11),
        _bar("chart-failed-by-cat",    "ds_by_category", "DQ_CATEGORY", "TOTAL_FAILED", "Category", "Failed Checks",  "Failed Checks by Category", 6, 11),
        _text("section-rules",   "### Per-Rule Detail",                   0, 16),
        _table("table-rule-detail",     "ds_rule_detail",     rule_cols,   list(zip(rule_cols, rule_hdrs)),   "Per-Rule Detail",  0, 17),
        _text("section-breach",  "### Breach Register - Rules below threshold", 0, 25),
        _table("table-breach-register", "ds_breach_register", breach_cols, list(zip(breach_cols, breach_hdrs)), "Breach Register",  0, 26, h=5),
    ]

    return {
        "datasets": datasets,
        "pages": [{
            "name": "dq_overview",
            "displayName": "DQ Dashboard",
            "pageType": "PAGE_TYPE_CANVAS",
            "layoutVersion": "GRID_V1",
            "layout": layout,
        }]
    }


def deploy_dashboard(host, token, entity, catalog, schema):
    ws_dir = f"/{WORKSPACE_USER}/DQ_Business_{entity}"
    dash_json = build_dashboard_json(entity, catalog, schema)

    payload = {
        "display_name": f"DQ Dashboard - {entity}",
        "parent_path":  f"/Workspace{ws_dir}",
        "serialized_dashboard": json.dumps(dash_json),
        "warehouse_id": WH_ID,
    }

    result = _db(host, token, "POST", "lakeview/dashboards", body=payload)
    dashboard_id = result.get("dashboard_id") or result.get("id", "")

    _db(host, token, "POST", f"lakeview/dashboards/{dashboard_id}/published",
        body={"warehouse_id": WH_ID})

    url = f"{host}/dashboards/{dashboard_id}"
    print(f"  Dashboard ID: {dashboard_id}")
    print(f"  Dashboard URL: {url}")
    return dashboard_id


# ── entity config loader ───────────────────────────────────────────────────────

def load_entity_config(entity):
    cfg_path = ENTITY_CFG_DIR / f"{entity}.py"
    if not cfg_path.exists():
        sys.exit(f"ERROR: No entity config found at {cfg_path}")
    spec   = importlib.util.spec_from_file_location(entity, cfg_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Onboard a new DQ entity end-to-end.")
    parser.add_argument("--entity",        required=True, help="Entity name, e.g. loan_gold")
    parser.add_argument("--catalog",       default=DEFAULT_CATALOG)
    parser.add_argument("--dq-schema",     default=DEFAULT_SCHEMA,  dest="dq_schema")
    parser.add_argument("--force-upload",  action="store_true", help="Re-upload shared notebooks even if they exist")
    parser.add_argument("--no-run",        action="store_true", help="Skip job trigger")
    parser.add_argument("--no-dashboard",  action="store_true", help="Skip dashboard deployment")
    args = parser.parse_args()

    entity   = args.entity
    catalog  = args.catalog
    dq_schema = args.dq_schema

    print(f"\n{'='*60}")
    print(f"  Onboarding entity: {entity}")
    print(f"  Catalog/Schema:    {catalog}.{dq_schema}")
    print(f"{'='*60}")

    # Load entity config
    config = load_entity_config(entity)
    print(f"\nLoaded config: {len(config.MAPPINGS)} mappings, {len(config.NEW_GENERIC_RULES)} new generic rules")

    # Generate 00_setup.py
    from generate_setup_notebook import generate
    setup_source = generate(config, catalog, dq_schema)
    print(f"Generated 00_setup.py ({len(setup_source)} chars)")

    # Credentials
    host, token = _load_credentials()

    # Upload shared notebooks (once)
    upload_shared_notebooks(host, token, force=args.force_upload)

    # Upload entity setup
    upload_entity_setup(host, token, entity, setup_source, force=True)

    # Create / update job
    job_id = create_or_update_job(host, token, entity, config, catalog, dq_schema)

    # Trigger run and wait
    if not args.no_run:
        trigger_and_wait(host, token, job_id)
    else:
        print("\n[--no-run] Skipping job trigger.")

    # Deploy dashboard
    if not args.no_dashboard:
        print("\n-- Dashboard --")
        dashboard_id = deploy_dashboard(host, token, entity, catalog, dq_schema)
    else:
        print("\n[--no-dashboard] Skipping dashboard deployment.")
        dashboard_id = None

    # Summary
    print(f"\n{'='*60}")
    print(f"  DONE: {entity}")
    print(f"  Job ID:        {job_id}")
    print(f"  Job URL:       {host}#job/{job_id}")
    if dashboard_id:
        print(f"  Dashboard ID:  {dashboard_id}")
        print(f"  Dashboard URL: {host}/dashboards/{dashboard_id}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    # Ensure generate_setup_notebook is importable from the same directory
    sys.path.insert(0, str(SCRIPT_DIR))
    main()
