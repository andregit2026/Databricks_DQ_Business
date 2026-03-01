import configparser, os, json, urllib.request, time

cfg = configparser.ConfigParser()
cfg.read(os.path.expanduser("~/.databrickscfg"))
d = cfg.defaults()
host  = d["host"].rstrip("/")
token = d["token"]
WH_ID = "94cbf5bc4ec73e30"
CAT   = "databricks_snippets_7405610928938750"
SCH   = "dbdemos_dq_business_arausch"

def sql_run(stmt, label=""):
    req = urllib.request.Request(
        f"{host}/api/2.0/sql/statements",
        data=json.dumps({
            "warehouse_id": WH_ID,
            "statement": stmt,
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
            "catalog": CAT,
            "schema": SCH,
        }).encode(),
        method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as r:
        result = json.loads(r.read())
    stmt_id = result.get("statement_id")
    deadline = time.time() + 90
    while result.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if time.time() > deadline: break
        time.sleep(2)
        with urllib.request.urlopen(urllib.request.Request(
            f"{host}/api/2.0/sql/statements/{stmt_id}",
            headers={"Authorization": f"Bearer {token}"})) as r:
            result = json.loads(r.read())
    state = result.get("status", {}).get("state", "?")
    rows = result.get("result", {}).get("data_array", [])
    err_msg = ""
    if state != "SUCCEEDED":
        err_msg = str(result.get("status", {}).get("error", {}))[:300]
    print(f"  [{label}] {state}: {rows[:2]} {err_msg}")
    return result

# List tables
sql_run("SHOW TABLES", "show tables")

# Check customer_gold_dq
sql_run("SELECT COUNT(*) FROM customer_gold_dq", "row count")
sql_run("SELECT DQ_RESULT FROM customer_gold_dq LIMIT 1", "sample DQ_RESULT")

# Test regexp_extract_all with capturing group
sql_run("""
    SELECT
        SUM(size(regexp_extract_all(DQ_RESULT, '(: [01])', 0))) AS total_checks,
        SUM(size(regexp_extract_all(DQ_RESULT, '(: 1 )',   0))) AS passed_partial
    FROM customer_gold_dq
""", "regexp_extract_all groups")

# Test filter+split
sql_run("""
    SELECT
        SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': [01]$'))) AS total_checks,
        SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': 1$')))    AS passed_checks
    FROM customer_gold_dq
""", "filter+split")

print("Done.")
