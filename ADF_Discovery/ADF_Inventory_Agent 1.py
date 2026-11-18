"""
Azure Data Factory Comprehensive Inventory Agent
-------------------------------------------------
Retrieves metadata for all ADF resources via Azure REST API using
Service Principal authentication (OAuth 2.0 client credentials).

No data is accessed — only resource configurations and definitions.

Resources collected:
  1. Pipelines          (with activity counts)
  2. Activities         (from pipelines, flattened)
  3. Data Flows         (with source/sink counts)
  4. Flowlets           (reusable data flow components)
  5. Datasets           (with linked service references)
  6. Linked Services    (connection definitions)
  7. Integration Runtimes (compute environments)
  8. Triggers           (scheduling configuration)
  9. Parameters         (factory & pipeline level)
 10. Variables          (factory & pipeline level)

Requirements:
    pip install requests openpyxl

Usage:
    python adf_inventory_agent.py
"""

import requests
import warnings
import time
from datetime import datetime
from io import BytesIO

# openpyxl is not part of the default kernel image — install it on first run.
try:
    import openpyxl  # noqa: F401
except ModuleNotFoundError:
    import subprocess
    import sys
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--quiet", "openpyxl"],
        check=True,
    )

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

warnings.filterwarnings("ignore")

# ── Configuration ─────────────────────────────────────────────────────────────
TENANT_ID         = "0631cc64-b7dd-4469-bed6-edf4399879a3"
CLIENT_ID         = "265a3cf1-ac02-4a08-a8a1-da047f2c3dd0"
CLIENT_SECRET     = "64b76c01-de43-4716-84fb-79d6542e8f45"
SUBSCRIPTION_ID   = "cd0659bf-d108-4169-acb1-3f26cef853af"
RESOURCE_GROUP    = "adf-glue-demo-rg"
FACTORY_NAME      = "adf-glue-demo-df"
OUTPUT_FILE       = "adf_inventory.xlsx"
# ─────────────────────────────────────────────────────────────────────────────

AZURE_MGMT_URL    = "https://management.azure.com"
ADF_API_VERSION   = "2018-06-01"
TOKEN_URL         = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
ADF_BASE          = (
    f"{AZURE_MGMT_URL}/subscriptions/{SUBSCRIPTION_ID}"
    f"/resourceGroups/{RESOURCE_GROUP}"
    f"/providers/Microsoft.DataFactory/factories/{FACTORY_NAME}"
)

# ── Auth ──────────────────────────────────────────────────────────────────────
_token_cache: dict = {}

def get_token() -> str:
    now = time.time()
    if _token_cache.get("token") and now < _token_cache.get("expires_at", 0) - 60:
        return _token_cache["token"]

    r = requests.post(TOKEN_URL, data={
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         "https://management.azure.com/.default",
    }, timeout=30, verify=False)

    if r.status_code != 200:
        raise RuntimeError(f"Auth failed [{r.status_code}]: {r.text}")

    payload = r.json()
    _token_cache["token"]      = payload["access_token"]
    _token_cache["expires_at"] = now + int(payload.get("expires_in", 3600))
    print(f"  [AUTH] Token obtained (expires in {payload.get('expires_in', 3600)}s)")
    return _token_cache["token"]


def get_headers() -> dict:
    return {
        "Authorization": f"Bearer {get_token()}",
        "Content-Type":  "application/json",
    }


# ── API helpers ───────────────────────────────────────────────────────────────
def api_get(path: str, params: dict = None) -> dict:
    p = {"api-version": ADF_API_VERSION}
    if params:
        p.update(params)
    url = f"{ADF_BASE}/{path.lstrip('/')}"
    try:
        r = requests.get(url, headers=get_headers(), params=p,
                         timeout=30, verify=False)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        print(f"  [WARN] {path} -> HTTP {e.response.status_code}: "
              f"{e.response.text[:120]}")
        return {}
    except Exception as e:
        print(f"  [ERROR] {path} -> {e}")
        return {}


def paginate(path: str) -> list[dict]:
    """Follow nextLink pagination and return all items."""
    items = []
    p = {"api-version": ADF_API_VERSION}
    url = f"{ADF_BASE}/{path.lstrip('/')}"
    while url:
        try:
            r = requests.get(url, headers=get_headers(), params=p,
                             timeout=30, verify=False)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"  [ERROR] paginate {path} -> {e}")
            break
        items.extend(data.get("value", []))
        url = data.get("nextLink")
        p   = {}   # nextLink already contains api-version
    return items


# ── Helpers ───────────────────────────────────────────────────────────────────
def _safe(v, fallback="N/A"):
    if v is None or v == "":
        return fallback
    return v


def _ts(iso: str) -> str:
    if not iso:
        return "N/A"
    try:
        return iso.replace("T", " ").split(".")[0].split("Z")[0]
    except Exception:
        return str(iso)


def _props(item: dict) -> dict:
    return item.get("properties", {})


def _str_list(lst) -> str:
    if not lst:
        return ""
    return ", ".join(str(x) for x in lst)


def _count(v) -> int:
    if isinstance(v, list):
        return len(v)
    if isinstance(v, dict):
        return len(v)
    return 0


# ── Resource Fetchers ─────────────────────────────────────────────────────────

def get_factory_info() -> list[dict]:
    """Factory-level metadata (single row)."""
    data = api_get("/") or {}
    p    = _props(data)
    gi   = p.get("globalParameters", {})
    rows = [{
        "Factory Name":         data.get("name", FACTORY_NAME),
        "Resource Group":       RESOURCE_GROUP,
        "Subscription ID":      SUBSCRIPTION_ID,
        "Location":             _safe(data.get("location")),
        "State":                _safe(p.get("provisioningState")),
        "Create Time":          _ts(p.get("createTime")),
        "Purview Account":      _safe(p.get("purviewConfiguration", {}).get("purviewResourceId", "N/A")),
        "Git Config Enabled":   "Yes" if p.get("repoConfiguration") else "No",
        "Repo Type":            _safe(p.get("repoConfiguration", {}).get("type", "N/A")),
        "Repo Name":            _safe(p.get("repoConfiguration", {}).get("repositoryName", "N/A")),
        "Branch":               _safe(p.get("repoConfiguration", {}).get("collaborationBranch", "N/A")),
        "Global Parameters":    _count(gi),
        "Public Network Access":_safe(p.get("publicNetworkAccess", "Enabled")),
        "Tags":                 str(data.get("tags", {})),
    }]
    return rows


def get_pipelines() -> list[dict]:
    items = paginate("pipelines")
    rows  = []
    for item in items:
        p           = _props(item)
        activities  = p.get("activities", [])
        params      = p.get("parameters", {})
        variables   = p.get("variables", {})
        folders     = p.get("folder", {})
        rows.append({
            "Pipeline Name":      item.get("name"),
            "Description":        _safe(p.get("description", ""), ""),
            "Folder":             _safe(folders.get("name", ""), "Root"),
            "Activity Count":     len(activities),
            "Activity Types":     _str_list(sorted({a.get("type","") for a in activities})),
            "Parameter Count":    _count(params),
            "Variable Count":     _count(variables),
            "Concurrency":        _safe(p.get("concurrency")),
            "Annotations":        _str_list(p.get("annotations", [])),
            "Last Publish Time":  _ts(item.get("etag", "")),
        })
    return rows


def get_activities() -> list[dict]:
    """Flatten all activities from all pipelines."""
    items = paginate("pipelines")
    rows  = []
    for item in items:
        pipeline_name = item.get("name", "")
        p             = _props(item)
        for act in p.get("activities", []):
            dep_on = act.get("dependsOn", [])
            depends = _str_list([d.get("activity", "") for d in dep_on])

            # Extract linked service ref if present
            ls_ref = ""
            for key in ("linkedServiceName", "linkedService"):
                ls = act.get(key, {})
                if isinstance(ls, dict):
                    ls_ref = ls.get("referenceName", "")
                    if ls_ref:
                        break

            # Extract dataset refs
            inputs  = act.get("inputs",  [])
            outputs = act.get("outputs", [])
            in_ds   = _str_list([i.get("referenceName", "") for i in inputs])
            out_ds  = _str_list([o.get("referenceName", "") for o in outputs])

            # Type-specific notes
            type_props = act.get("typeProperties", {})
            notes = []
            if "url" in type_props:
                notes.append(f"url={str(type_props['url'])[:60]}")
            if "pipeline" in type_props:
                notes.append(f"calls={type_props['pipeline'].get('referenceName','')}")
            if "dataflow" in type_props:
                notes.append(f"dataflow={type_props['dataflow'].get('referenceName','')}")
            if "waitTimeInSeconds" in type_props:
                notes.append(f"wait={type_props['waitTimeInSeconds']}s")
            if "expression" in type_props:
                expr = type_props["expression"]
                if isinstance(expr, dict):
                    notes.append(f"expr={str(expr.get('value',''))[:60]}")

            rows.append({
                "Pipeline Name":    pipeline_name,
                "Activity Name":    act.get("name", ""),
                "Activity Type":    act.get("type", ""),
                "Description":      _safe(act.get("description", ""), ""),
                "Depends On":       depends,
                "Input Datasets":   in_ds,
                "Output Datasets":  out_ds,
                "Linked Service":   ls_ref,
                "Notes":            "; ".join(notes),
                "Policy Timeout":   _safe(act.get("policy", {}).get("timeout", ""), ""),
                "Policy Retries":   _safe(act.get("policy", {}).get("retry", ""), ""),
            })
    return rows


def get_dataflows() -> list[dict]:
    items = paginate("dataflows")
    rows  = []
    for item in items:
        p    = _props(item)
        kind = p.get("type", "MappingDataFlow")
        tf   = p.get("typeProperties", {})
        sources    = tf.get("sources", [])
        sinks      = tf.get("sinks", [])
        transforms = tf.get("transformations", [])
        script     = tf.get("script", "") or ""
        rows.append({
            "DataFlow Name":      item.get("name"),
            "Type":               kind,
            "Description":        _safe(p.get("description", ""), ""),
            "Folder":             _safe(p.get("folder", {}).get("name", ""), "Root"),
            "Source Count":       len(sources),
            "Sources":            _str_list([s.get("name","") for s in sources]),
            "Sink Count":         len(sinks),
            "Sinks":              _str_list([s.get("name","") for s in sinks]),
            "Transformation Count": len(transforms),
            "Transformations":    _str_list([t.get("name","") for t in transforms]),
            "Script Lines":       len(script.splitlines()) if script else 0,
            "Annotations":        _str_list(p.get("annotations", [])),
        })
    return rows


def get_flowlets() -> list[dict]:
    """Flowlets share the /dataflows endpoint but have type=Flowlet."""
    items = paginate("dataflows")
    rows  = []
    for item in items:
        p    = _props(item)
        kind = p.get("type", "")
        if kind.lower() != "flowlet":
            continue
        tf      = p.get("typeProperties", {})
        sources = tf.get("sources", [])
        sinks   = tf.get("sinks", [])
        script  = tf.get("script", "") or ""
        rows.append({
            "Flowlet Name":       item.get("name"),
            "Description":        _safe(p.get("description", ""), ""),
            "Folder":             _safe(p.get("folder", {}).get("name", ""), "Root"),
            "Source Count":       len(sources),
            "Sources":            _str_list([s.get("name","") for s in sources]),
            "Sink Count":         len(sinks),
            "Sinks":              _str_list([s.get("name","") for s in sinks]),
            "Script Lines":       len(script.splitlines()) if script else 0,
            "Annotations":        _str_list(p.get("annotations", [])),
        })
    return rows or [{"Note": "No Flowlets found in this factory."}]


def get_datasets() -> list[dict]:
    items = paginate("datasets")
    rows  = []
    for item in items:
        p    = _props(item)
        ls   = p.get("linkedServiceName", {})
        tp   = p.get("typeProperties", {})
        schema = p.get("schema", [])
        params = p.get("parameters", {})

        # Extract location / table / path hints
        loc   = tp.get("location", {})
        hints = []
        for fld in ("tableName", "table", "fileName", "container",
                    "relativeUrl", "fileSystem", "bucketName"):
            val = tp.get(fld) or loc.get(fld)
            if val and isinstance(val, str):
                hints.append(f"{fld}={val[:50]}")

        rows.append({
            "Dataset Name":        item.get("name"),
            "Dataset Type":        p.get("type", ""),
            "Description":         _safe(p.get("description", ""), ""),
            "Folder":              _safe(p.get("folder", {}).get("name", ""), "Root"),
            "Linked Service":      _safe(ls.get("referenceName", "")),
            "Linked Service Type": _safe(ls.get("type", "")),
            "Schema Column Count": len(schema),
            "Parameter Count":     _count(params),
            "Location Type":       _safe(loc.get("type", "")),
            "Location Hints":      "; ".join(hints),
            "Annotations":         _str_list(p.get("annotations", [])),
        })
    return rows


def get_linked_services() -> list[dict]:
    items = paginate("linkedservices")
    rows  = []
    for item in items:
        p    = _props(item)
        tp   = p.get("typeProperties", {})
        ir   = p.get("connectVia", {})
        params = p.get("parameters", {})

        # Redact secrets — capture only non-secret config fields
        safe_fields = []
        secret_fields = ("password", "servicePrincipalKey", "accountKey",
                         "sasToken", "connectionString", "clientSecret",
                         "credential", "encryptedCredential")
        for k, v in tp.items():
            if any(s in k.lower() for s in secret_fields):
                safe_fields.append(f"{k}=[REDACTED]")
            elif isinstance(v, (str, int, float, bool)):
                safe_fields.append(f"{k}={str(v)[:60]}")

        rows.append({
            "Linked Service Name":    item.get("name"),
            "Type":                   p.get("type", ""),
            "Description":            _safe(p.get("description", ""), ""),
            "Integration Runtime":    _safe(ir.get("referenceName", "AutoResolve")),
            "Parameter Count":        _count(params),
            "Connection Properties":  "; ".join(safe_fields[:6]),
            "Annotations":            _str_list(p.get("annotations", [])),
        })
    return rows


def get_integration_runtimes() -> list[dict]:
    items = paginate("integrationruntimes")
    rows  = []
    for item in items:
        p    = _props(item)
        tp   = p.get("typeProperties", {})
        kind = p.get("type", "")
        rows.append({
            "IR Name":                   item.get("name"),
            "Type":                      kind,
            "Description":               _safe(p.get("description", ""), ""),
            "Location":                  _safe(tp.get("location", "AutoResolve")),
            "Node Size":                 _safe(tp.get("nodeSize", "")),
            "Node Count":                _safe(tp.get("nodeCount", "")),
            "Max Parallel Exec/Node":    _safe(tp.get("maxParallelExecutionsPerNode", "")),
            "Compute Type":              _safe(tp.get("computeProperties", {}).get("location", "")),
            "Data Flow Compute":         _safe(tp.get("dataFlowProperties", {}).get("computeType", "")),
            "VNet Enabled":              "Yes" if tp.get("customerVirtualNetwork") else "No",
            "VNet Subnet":               _safe(tp.get("customerVirtualNetwork", {}).get("subnetId", "N/A")),
            "Edition":                   _safe(tp.get("edition", "")),
            "License Type":              _safe(tp.get("licenseType", "")),
            "Catalog DB":                _safe(tp.get("ssisProperties", {})
                                               .get("catalogInfo", {})
                                               .get("catalogServerEndpoint", "N/A")),
            "Auto Update":               _safe(tp.get("autoUpdate", "")),
            "Linked IR Name":            _safe(tp.get("linkedInfo", {}).get("authorizationType", "")),
        })
    return rows


def get_triggers() -> list[dict]:
    items = paginate("triggers")
    rows  = []
    for item in items:
        p    = _props(item)
        tp   = p.get("typeProperties", {})
        kind = p.get("type", "")
        pipelines_refs = p.get("pipelines", [])

        # Schedule / recurrence
        recurrence  = tp.get("recurrence", {})
        schedule    = ""
        if recurrence:
            freq     = recurrence.get("frequency", "")
            interval = recurrence.get("interval", "")
            schedule = f"Every {interval} {freq}" if freq else ""

        # Tumbling window / Event
        if kind == "TumblingWindowTrigger":
            freq     = tp.get("frequency", "")
            interval = tp.get("interval", "")
            schedule = f"TumblingWindow: every {interval} {freq}"
        if kind == "BlobEventsTrigger":
            schedule = f"Event: {tp.get('events', [])}"
        if kind == "CustomEventsTrigger":
            schedule = f"Custom event: {tp.get('subject', '')}"

        rows.append({
            "Trigger Name":       item.get("name"),
            "Type":               kind,
            "Description":        _safe(p.get("description", ""), ""),
            "Runtime State":      _safe(p.get("runtimeState", "")),
            "Schedule":           schedule,
            "Start Time":         _ts(tp.get("startTime") or recurrence.get("startTime","")),
            "End Time":           _ts(tp.get("endTime") or recurrence.get("endTime","")),
            "Timezone":           _safe(tp.get("timeZone") or recurrence.get("timeZone","")),
            "Pipeline Count":     len(pipelines_refs),
            "Pipelines":          _str_list([pp.get("pipelineReference",{})
                                             .get("referenceName","") for pp in pipelines_refs]),
            "Annotations":        _str_list(p.get("annotations", [])),
        })
    return rows


def get_parameters() -> list[dict]:
    """Factory-level global parameters + pipeline-level parameters."""
    rows = []

    # ── Factory global parameters ─────────────────────────────────────────────
    factory = api_get("/") or {}
    global_params = _props(factory).get("globalParameters", {})
    for name, meta in global_params.items():
        rows.append({
            "Scope":         "Factory (Global)",
            "Pipeline Name": FACTORY_NAME,
            "Parameter Name": name,
            "Type":          _safe(meta.get("type", "")),
            "Default Value": _safe(str(meta.get("value", "")), ""),
            "Description":   "",
        })

    # ── Pipeline-level parameters ─────────────────────────────────────────────
    pipelines = paginate("pipelines")
    for pl in pipelines:
        pl_name = pl.get("name", "")
        params  = _props(pl).get("parameters", {})
        for pname, pmeta in params.items():
            rows.append({
                "Scope":          "Pipeline",
                "Pipeline Name":  pl_name,
                "Parameter Name": pname,
                "Type":           _safe(pmeta.get("type", "")),
                "Default Value":  _safe(str(pmeta.get("defaultValue", "")), ""),
                "Description":    _safe(pmeta.get("description", ""), ""),
            })

    return rows or [{"Note": "No parameters found."}]


def get_variables() -> list[dict]:
    """Pipeline-level variables (ADF doesn't have factory-level variables)."""
    rows     = []
    pipelines = paginate("pipelines")
    for pl in pipelines:
        pl_name   = pl.get("name", "")
        variables = _props(pl).get("variables", {})
        for vname, vmeta in variables.items():
            rows.append({
                "Pipeline Name":  pl_name,
                "Variable Name":  vname,
                "Type":           _safe(vmeta.get("type", "")),
                "Default Value":  _safe(str(vmeta.get("defaultValue", "")), ""),
            })
    return rows or [{"Note": "No pipeline variables found."}]


# ── Complexity Classification ─────────────────────────────────────────────────

COMPLEXITY_COLORS = {
    "SIMPLE":  "70AD47",
    "MEDIUM":  "FFB900",
    "COMPLEX": "C00000",
}
COMPLEXITY_ORDER = {"SIMPLE": 0, "MEDIUM": 1, "COMPLEX": 2}


def _max_c(*levels: str) -> str:
    return max(levels, key=lambda x: COMPLEXITY_ORDER.get(x, 0))


def classify_pipeline(row: dict) -> tuple[str, str]:
    act  = int(row.get("Activity Count") or 0)
    p    = int(row.get("Parameter Count") or 0)
    v    = int(row.get("Variable Count") or 0)
    conc = row.get("Concurrency")

    reasons = []
    level   = "SIMPLE"
    if act >= 15:
        level = _max_c(level, "COMPLEX"); reasons.append(f"{act} activities (>=15)")
    if act >= 5 and level != "COMPLEX":
        level = _max_c(level, "MEDIUM"); reasons.append(f"{act} activities (5-14)")
    if p >= 5:
        level = _max_c(level, "MEDIUM"); reasons.append(f"{p} parameters")
    if v >= 5:
        level = _max_c(level, "MEDIUM"); reasons.append(f"{v} variables")
    if conc and str(conc) not in ("N/A", ""):
        level = _max_c(level, "MEDIUM"); reasons.append("concurrency set")
    return level, "; ".join(reasons) or "simple linear pipeline"


def classify_activity(row: dict) -> tuple[str, str]:
    atype   = str(row.get("Activity Type") or "").lower()
    depends = str(row.get("Depends On") or "")
    retries = row.get("Policy Retries")
    timeout = row.get("Policy Timeout")

    dep_count = len([d for d in depends.split(",") if d.strip()])
    complex_types = ("executepipeline", "executedataflow", "hdinsight",
                     "databricks", "custom", "azurefunction",
                     "webactivity", "script")
    medium_types  = ("lookup", "getmetadata", "filter", "foreach",
                     "until", "ifcondition", "switch", "wait", "setVariable")

    reasons = []
    level   = "SIMPLE"
    if any(t in atype for t in complex_types):
        level = _max_c(level, "COMPLEX"); reasons.append(f"type: {atype}")
    if dep_count >= 3:
        level = _max_c(level, "COMPLEX"); reasons.append(f"{dep_count} dependencies")
    if level != "COMPLEX":
        if any(t in atype for t in [m.lower() for m in medium_types]):
            level = _max_c(level, "MEDIUM"); reasons.append(f"type: {atype}")
        if dep_count in (1, 2):
            level = _max_c(level, "MEDIUM"); reasons.append(f"{dep_count} dependency")
        if retries and str(retries) not in ("N/A", "", "0"):
            level = _max_c(level, "MEDIUM"); reasons.append(f"retries={retries}")
        if timeout and str(timeout) not in ("N/A", "", "7.00:00:00"):
            level = _max_c(level, "MEDIUM"); reasons.append("custom timeout")
    return level, "; ".join(reasons) or "simple copy/data activity"


def classify_dataflow(row: dict) -> tuple[str, str]:
    sources = int(row.get("Source Count") or 0)
    sinks   = int(row.get("Sink Count") or 0)
    trans   = int(row.get("Transformation Count") or 0)
    lines   = int(row.get("Script Lines") or 0)

    reasons = []
    level   = "SIMPLE"
    if trans >= 10 or lines >= 200:
        level = _max_c(level, "COMPLEX"); reasons.append(f"{trans} transforms, {lines} script lines")
    if sources + sinks >= 5:
        level = _max_c(level, "COMPLEX"); reasons.append(f"{sources} sources, {sinks} sinks")
    if level != "COMPLEX":
        if trans >= 4:
            level = _max_c(level, "MEDIUM"); reasons.append(f"{trans} transformations")
        if sources + sinks in (3, 4):
            level = _max_c(level, "MEDIUM"); reasons.append(f"{sources} sources, {sinks} sinks")
    return level, "; ".join(reasons) or "simple 1-source 1-sink data flow"


def classify_dataset(row: dict) -> tuple[str, str]:
    dtype   = str(row.get("Dataset Type") or "").lower()
    schema  = int(row.get("Schema Column Count") or 0)
    params  = int(row.get("Parameter Count") or 0)

    complex_ds = ("delta", "orc", "avro", "parquet", "binary", "xml")
    medium_ds  = ("json", "excel", "snowflake", "googlesheetsdataset")

    reasons = []
    level   = "SIMPLE"
    if any(t in dtype for t in complex_ds):
        level = _max_c(level, "COMPLEX"); reasons.append(f"type: {dtype}")
    if schema >= 30:
        level = _max_c(level, "COMPLEX"); reasons.append(f"{schema} schema columns")
    if level != "COMPLEX":
        if any(t in dtype for t in medium_ds):
            level = _max_c(level, "MEDIUM"); reasons.append(f"type: {dtype}")
        if schema >= 10:
            level = _max_c(level, "MEDIUM"); reasons.append(f"{schema} schema columns")
        if params >= 2:
            level = _max_c(level, "MEDIUM"); reasons.append(f"{params} parameters")
    return level, "; ".join(reasons) or "simple flat dataset"


def classify_linked_service(row: dict) -> tuple[str, str]:
    ltype = str(row.get("Type") or "").lower()
    ir    = str(row.get("Integration Runtime") or "").lower()

    complex_ls = ("snowflake", "databricks", "hdinsight", "googlebigquery",
                  "amazonredshift", "sap", "oracle", "teradata",
                  "cosmosdb", "dynamics", "salesforce")
    medium_ls  = ("sqlserver", "azuresql", "mysql", "postgresql",
                  "restservice", "web", "odata", "azuredatalake",
                  "azuredatabricks", "azurefileshare")

    reasons = []
    level   = "SIMPLE"
    if any(t in ltype for t in complex_ls):
        level = _max_c(level, "COMPLEX"); reasons.append(f"type: {ltype}")
    if "selfhosted" in ir:
        level = _max_c(level, "COMPLEX"); reasons.append("Self-Hosted IR")
    if level != "COMPLEX":
        if any(t in ltype for t in medium_ls):
            level = _max_c(level, "MEDIUM"); reasons.append(f"type: {ltype}")
    return level, "; ".join(reasons) or "simple cloud linked service"


def classify_ir(row: dict) -> tuple[str, str]:
    ir_type = str(row.get("Type") or "").lower()
    vnet    = row.get("VNet Enabled", "No")
    nodes   = row.get("Node Count")
    catalog = str(row.get("Catalog DB") or "")

    reasons = []
    level   = "SIMPLE"
    if "selfhosted" in ir_type:
        level = _max_c(level, "COMPLEX"); reasons.append("Self-Hosted IR")
    if catalog not in ("", "N/A"):
        level = _max_c(level, "COMPLEX"); reasons.append("SSIS catalog configured")
    if level != "COMPLEX":
        if vnet == "Yes":
            level = _max_c(level, "MEDIUM"); reasons.append("VNet-managed IR")
        if nodes and str(nodes) not in ("", "N/A") and int(str(nodes)) > 1:
            level = _max_c(level, "MEDIUM"); reasons.append(f"{nodes} nodes")
    return level, "; ".join(reasons) or "auto-resolve managed IR"


def classify_trigger(row: dict) -> tuple[str, str]:
    kind     = str(row.get("Type") or "").lower()
    pl_count = int(row.get("Pipeline Count") or 0)
    state    = str(row.get("Runtime State") or "").lower()

    reasons = []
    level   = "SIMPLE"
    if "customevent" in kind or "blobevent" in kind:
        level = _max_c(level, "COMPLEX"); reasons.append(f"event-based trigger: {kind}")
    if pl_count >= 3:
        level = _max_c(level, "COMPLEX"); reasons.append(f"triggers {pl_count} pipelines")
    if level != "COMPLEX":
        if "tumbling" in kind:
            level = _max_c(level, "MEDIUM"); reasons.append("tumbling window trigger")
        if pl_count == 2:
            level = _max_c(level, "MEDIUM"); reasons.append("triggers 2 pipelines")
        if state == "stopped":
            level = _max_c(level, "MEDIUM"); reasons.append("trigger is stopped")
    return level, "; ".join(reasons) or "simple schedule trigger"


CLASSIFIERS = {
    "Pipelines":             classify_pipeline,
    "Activities":            classify_activity,
    "Data Flows":            classify_dataflow,
    "Datasets":              classify_dataset,
    "Linked Services":       classify_linked_service,
    "Integration Runtimes":  classify_ir,
    "Triggers":              classify_trigger,
}


def apply_classification(resource_type: str, rows: list[dict]) -> list[dict]:
    classifier = CLASSIFIERS.get(resource_type)
    if not classifier:
        return rows
    enriched = []
    for row in rows:
        if "Error" in row or "Note" in row:
            enriched.append(row); continue
        try:
            level, reason = classifier(row)
        except Exception as exc:
            level, reason = "SIMPLE", f"error: {exc}"
        new_row = {"Complexity": level, "Complexity Reason": reason}
        new_row.update(row)
        enriched.append(new_row)
    return enriched


# ── Excel Styling ─────────────────────────────────────────────────────────────

THEME = {
    "header_bg":   "0078D4",   # Azure blue
    "header_fg":   "FFFFFF",
    "accent":      "005A9E",
    "alt_row":     "EBF3FB",
    "border_color":"B0C4DE",
    "font_name":   "Arial",
}


def _style_header(ws, row: int, ncols: int):
    fill   = PatternFill("solid", fgColor=THEME["header_bg"])
    font   = Font(name=THEME["font_name"], bold=True, color=THEME["header_fg"], size=10)
    border = Border(bottom=Side(style="medium", color=THEME["accent"]))
    for col in range(1, ncols + 1):
        cell = ws.cell(row=row, column=col)
        cell.fill      = fill
        cell.font      = font
        cell.border    = border
        cell.alignment = Alignment(horizontal="center", vertical="center",
                                   wrap_text=True)


def _style_data_rows(ws, start_row: int, nrows: int, ncols: int):
    thin   = Side(style="thin", color=THEME["border_color"])
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    alt    = PatternFill("solid", fgColor=THEME["alt_row"])
    font   = Font(name=THEME["font_name"], size=9)
    for r in range(start_row, start_row + nrows):
        for c in range(1, ncols + 1):
            cell            = ws.cell(row=r, column=c)
            cell.font       = font
            cell.border     = border
            cell.alignment  = Alignment(vertical="center")
            if r % 2 == 0:
                cell.fill = alt


def _auto_col_width(ws):
    for col in ws.columns:
        max_len    = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            try:
                if cell.value:
                    max_len = max(max_len, len(str(cell.value)))
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(max(max_len + 4, 12), 60)


def write_sheet(wb: Workbook, name: str, rows: list[dict], tab_color: str):
    safe = name.replace("/", "-").replace("\\", "-")[:31]
    ws   = wb.create_sheet(title=safe)
    ws.tab_color           = tab_color
    ws.sheet_view.showGridLines = False
    ws.freeze_panes        = "A2"

    if not rows:
        ws.append(["No data retrieved"]); return

    headers      = list(rows[0].keys())
    complex_col  = headers.index("Complexity") + 1 if "Complexity" in headers else None

    ws.append(headers)
    _style_header(ws, 1, len(headers))
    ws.row_dimensions[1].height = 22

    for r_idx, row in enumerate(rows, start=2):
        ws.append([row.get(h, "") for h in headers])
        if complex_col:
            cell  = ws.cell(row=r_idx, column=complex_col)
            level = str(row.get("Complexity", "")).upper()
            color = COMPLEXITY_COLORS.get(level)
            if color:
                cell.fill = PatternFill("solid", fgColor=color)
                cell.font = Font(
                    name=THEME["font_name"], size=9, bold=True,
                    color="FFFFFF" if level == "COMPLEX" else "000000",
                )
                cell.alignment = Alignment(horizontal="center", vertical="center")

    _style_data_rows(ws, 2, len(rows), len(headers))
    _auto_col_width(ws)

    if len(rows) > 0 and len(headers) <= 26:
        try:
            tbl = Table(
                displayName=safe.replace(" ", "_").replace("-", "_")[:30],
                ref=f"A1:{get_column_letter(len(headers))}{len(rows)+1}",
            )
            tbl.tableStyleInfo = TableStyleInfo(
                name="TableStyleMedium2",
                showRowStripes=True, showColumnStripes=False,
            )
            ws.add_table(tbl)
        except Exception:
            pass


def write_summary(wb: Workbook, summary: list[dict]):
    ws = wb.create_sheet(title="Summary", index=0)
    ws.tab_color            = "0078D4"
    ws.sheet_view.showGridLines = False

    # Title
    ws.merge_cells("A1:F1")
    t = ws["A1"]
    t.value     = "Azure Data Factory — Resource Inventory"
    t.font      = Font(name="Arial", bold=True, size=18, color="FFFFFF")
    t.fill      = PatternFill("solid", fgColor="0078D4")
    t.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 38

    ws.merge_cells("A2:F2")
    s = ws["A2"]
    s.value     = (f"Factory: {FACTORY_NAME}   |   "
                   f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    s.font      = Font(name="Arial", size=9, italic=True, color="FFFFFF")
    s.fill      = PatternFill("solid", fgColor="005A9E")
    s.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[2].height = 16

    ws.append([])

    headers = ["Resource Type", "Count", "Simple", "Medium", "Complex", "Sheet"]
    ws.append(headers)
    _style_header(ws, 4, 6)

    thin   = Side(style="thin", color="B0C4DE")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    alt    = PatternFill("solid", fgColor="EBF3FB")

    for i, row in enumerate(summary):
        ws.append([
            row["Resource Type"], row["Count"],
            row.get("Simple", ""), row.get("Medium", ""), row.get("Complex", ""),
            row["Sheet"],
        ])
        for c in range(1, 7):
            cell           = ws.cell(row=5 + i, column=c)
            cell.font      = Font(name="Arial", size=10)
            cell.border    = border
            cell.alignment = Alignment(vertical="center")
            if (5 + i) % 2 == 0:
                cell.fill = alt

    last = 5 + len(summary)
    ws.append(["TOTAL",
               f"=SUM(B5:B{last-1})",
               f"=SUM(C5:C{last-1})",
               f"=SUM(D5:D{last-1})",
               f"=SUM(E5:E{last-1})",
               ""])
    for c in range(1, 7):
        cell       = ws.cell(row=last, column=c)
        cell.font  = Font(name="Arial", bold=True, size=10, color="FFFFFF")
        cell.fill  = PatternFill("solid", fgColor="0078D4")
        cell.border = border

    # Colour the Simple/Medium/Complex header cells
    for col, color, txt in [("C", "70AD47","000000"),
                             ("D", "FFB900","000000"),
                             ("E", "C00000","FFFFFF")]:
        cell       = ws[f"{col}4"]
        cell.fill  = PatternFill("solid", fgColor=color)
        cell.font  = Font(name="Arial", bold=True, size=10, color=txt)
        cell.alignment = Alignment(horizontal="center", vertical="center")

    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["B"].width = 10
    ws.column_dimensions["C"].width = 10
    ws.column_dimensions["D"].width = 10
    ws.column_dimensions["E"].width = 10
    ws.column_dimensions["F"].width = 28


# ── Resource registry ─────────────────────────────────────────────────────────

RESOURCE_REGISTRY = [
    ("Factory Info",        get_factory_info,        "0078D4"),
    ("Pipelines",           get_pipelines,            "005A9E"),
    ("Activities",          get_activities,           "003F6E"),
    ("Data Flows",          get_dataflows,            "E81123"),
    ("Flowlets",            get_flowlets,             "FF8C00"),
    ("Datasets",            get_datasets,             "107C10"),
    ("Linked Services",     get_linked_services,      "B4009E"),
    ("Integration Runtimes",get_integration_runtimes, "004E8C"),
    ("Triggers",            get_triggers,             "D83B01"),
    ("Parameters",          get_parameters,           "498205"),
    ("Variables",           get_variables,            "038387"),
]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  Azure Data Factory Inventory Agent")
    print(f"  Factory : {FACTORY_NAME}")
    print(f"  RG      : {RESOURCE_GROUP}")
    print(f"  Time    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 62)

    wb      = Workbook()
    wb.remove(wb.active)
    summary = []
    all_classified = []

    for name, fetcher, tab_color in RESOURCE_REGISTRY:
        print(f"\n▶  Fetching {name} ...")
        try:
            rows  = fetcher()
            count = len(rows)
            print(f"   ✓  {count} record(s) found")
        except Exception as e:
            print(f"   ✗  Error: {e}")
            rows  = [{"Error": str(e)}]
            count = 0

        rows    = apply_classification(name, rows)
        simple  = sum(1 for r in rows if r.get("Complexity") == "SIMPLE")
        medium  = sum(1 for r in rows if r.get("Complexity") == "MEDIUM")
        complex_= sum(1 for r in rows if r.get("Complexity") == "COMPLEX")

        write_sheet(wb, name, rows, tab_color)
        summary.append({
            "Resource Type": name,
            "Count":   count,
            "Simple":  simple   if simple   else "",
            "Medium":  medium   if medium   else "",
            "Complex": complex_ if complex_ else "",
            "Sheet":   name[:31],
        })

        for r in rows:
            if "Complexity" in r:
                all_classified.append({
                    "Resource Type":     name,
                    "Complexity":        r.get("Complexity", ""),
                    "Complexity Reason": r.get("Complexity Reason", ""),
                    "Name / ID": (
                        r.get("Pipeline Name") or r.get("Activity Name") or
                        r.get("DataFlow Name") or r.get("Flowlet Name") or
                        r.get("Dataset Name")  or r.get("Linked Service Name") or
                        r.get("IR Name")        or r.get("Trigger Name") or
                        r.get("Parameter Name") or r.get("Variable Name") or
                        r.get("Factory Name")   or ""
                    ),
                })

    if all_classified:
        write_sheet(wb, "Classification Overview", all_classified, "0078D4")
        summary.append({
            "Resource Type": "Classification Overview",
            "Count":   len(all_classified),
            "Simple":  "", "Medium":  "", "Complex": "",
            "Sheet":   "Classification Overview",
        })

    write_summary(wb, summary)
    sheets = wb.sheetnames
    wb.move_sheet("Summary", offset=-len(sheets) + 1)

    # The workspace stage mount does not support the random-access seeks that
    # openpyxl's zip writer needs, so build the workbook in memory and flush it
    # out with a single sequential write.
    buffer = BytesIO()
    wb.save(buffer)
    with open(OUTPUT_FILE, "wb") as fh:
        fh.write(buffer.getvalue())

    print(f"\n{'='*62}")
    print(f"  ✅  Inventory saved → {OUTPUT_FILE}")
    print(f"{'='*62}\n")


if __name__ == "__main__":
    main()
