"""
Deploy DQ notebooks to Databricks workspace via REST API.
Reads credentials from ~/.databrickscfg.
Uploads 4 notebooks to /Users/andre.r23@gmx.net/DQ_Business/
Creates a Databricks Job with 3 tasks (setup -> apply_dq -> aggregate).
"""
import configparser, os, sys, json, base64
import urllib.request, urllib.error

# -- Read credentials --------------------------------------------------------
cfg = configparser.ConfigParser()
cfg.read(os.path.expanduser("~/.databrickscfg"))
section = cfg.sections()[0] if cfg.sections() else "DEFAULT"
host  = cfg[section].get("host", "").rstrip("/")
token = cfg[section].get("token", "")

if not host or not token:
    print("ERROR: Could not read host/token from ~/.databrickscfg")
    sys.exit(1)

print(f"Workspace: {host.replace(host.split('//')[1].split('.')[0], '***')}")

# -- Helper: Databricks REST call -------------------------------------------
def db_request(method, path, body=None):
    url = f"{host}/api/2.0/{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"  HTTP {e.code} on {method} {path}: {err[:200]}")
        raise

# -- Create workspace directory ---------------------------------------------
workspace_dir = "/Users/andre.r23@gmx.net/DQ_Business"
print(f"\nCreating workspace directory: {workspace_dir}")
try:
    db_request("POST", "workspace/mkdirs", {"path": workspace_dir})
    print("  Directory created (or already exists).")
except Exception as e:
    print(f"  mkdirs: {e}")

# -- Upload notebooks --------------------------------------------------------
notebooks_dir = os.path.dirname(os.path.abspath(__file__))
notebooks = [
    ("00_setup.py",                 "00_setup"),
    ("01_apply_dq_rules.py",        "01_apply_dq_rules"),
    ("02_aggregate_dq_results.py",  "02_aggregate_dq_results"),
    ("03_dq_dashboard.py",          "03_dq_dashboard"),
]

uploaded = {}
print("\nUploading notebooks...")
for filename, nb_name in notebooks:
    local_path = os.path.join(notebooks_dir, filename)
    ws_path    = f"{workspace_dir}/{nb_name}"

    with open(local_path, "rb") as f:
        content_b64 = base64.b64encode(f.read()).decode()

    body = {
        "path":      ws_path,
        "format":    "SOURCE",
        "language":  "PYTHON",
        "content":   content_b64,
        "overwrite": True
    }
    db_request("POST", "workspace/import", body)
    uploaded[nb_name] = ws_path
    print(f"  Uploaded: {ws_path}")

# -- Create Databricks Job ---------------------------------------------------
CATALOG    = "databricks_snippets_7405610928938750"
DQ_SCHEMA  = "dbdemos_dq_business_arausch"
SRC_CAT    = "databricks_snippets_7405610928938750"
SRC_SCHEMA = "dbdemos_fsi_credit"
TABLE      = "customer_gold"

common_params = {
    "catalog":      CATALOG,
    "dq_schema":    DQ_SCHEMA,
    "target_table": TABLE,
}

job_payload = {
    "name": f"DQ_{TABLE}",
    "tasks": [
        {
            "task_key": "setup",
            "notebook_task": {
                "notebook_path": uploaded["00_setup"],
                "base_parameters": {
                    **common_params,
                    "source_catalog": SRC_CAT,
                    "source_schema":  SRC_SCHEMA,
                }
            }
        },
        {
            "task_key": "apply_dq_rules",
            "depends_on": [{"task_key": "setup"}],
            "notebook_task": {
                "notebook_path": uploaded["01_apply_dq_rules"],
                "base_parameters": common_params
            }
        },
        {
            "task_key": "aggregate_dq_results",
            "depends_on": [{"task_key": "apply_dq_rules"}],
            "notebook_task": {
                "notebook_path": uploaded["02_aggregate_dq_results"],
                "base_parameters": common_params
            }
        },
        {
            "task_key": "dq_dashboard",
            "depends_on": [{"task_key": "aggregate_dq_results"}],
            "notebook_task": {
                "notebook_path": uploaded["03_dq_dashboard"],
                "base_parameters": {
                    "catalog":   CATALOG,
                    "dq_schema": DQ_SCHEMA,
                }
            }
        },
    ],
    "queue": {"enabled": True},
}

# Check if job already exists (search by name)
existing_jobs = db_request("GET", "jobs/list")
existing_id = None
for j in existing_jobs.get("jobs", []):
    if j.get("settings", {}).get("name") == job_payload["name"]:
        existing_id = j["job_id"]
        break

if existing_id:
    print(f"\nUpdating existing job (id={existing_id}): {job_payload['name']}")
    db_request("POST", "jobs/reset", {"job_id": existing_id, "new_settings": job_payload})
    job_id = existing_id
else:
    print(f"\nCreating new job: {job_payload['name']}")
    result = db_request("POST", "jobs/create", job_payload)
    job_id = result["job_id"]

print(f"Job ID: {job_id}")
print(f"\nDone. To run the job:")
print(f"  POST {host}/api/2.0/jobs/run-now  {{\"job_id\": {job_id}}}")
print(f"\nOr open in browser:")
print(f"  {host}#job/{job_id}")

# Write job_id to a file for reference
with open(os.path.join(notebooks_dir, "job_id.txt"), "w") as f:
    f.write(str(job_id))
print(f"\nJob ID saved to job_id.txt")
