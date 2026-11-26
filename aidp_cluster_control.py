# ============================================================================================
# AIDP CLUSTER CONTROL AND RESTART TIMER
# Runs in a notebook on one AIDP cluster and controls a DIFFERENT cluster through the
# AIDP REST data plane API (version 20260430). Uses the same service-account oci_config
# from the AIDP Volume as the Object Storage upload script.
#
# --------------------------------------------------------------------------------------------
# CHANGE HISTORY:
# --------------------------------------------------------------------------------------------
# Modification log, newest first: date | JIRA | change.
# --------------------------------------------------------------------------------------------
#
# 2026-11-26 | n/a | Runs as the Entra-domain user from aidp_cluster_control_config.json
#   (the original aidp_job_config.json service account is not recognized by AIDP: 404
#   NotAuthorizedOrNotFound). Key file path checked before signing; signing user printed.
#   Tag: n/a (2026-11-26c). The key_file path is now used exactly as given
#   (no "/" added); a missing key lists the folder contents. Tag: n/a (2026-11-26d).
#   Key file moved next to the config file; if key_file is not found as given, the same
#   file name is looked up in the config file's folder. Tag: n/a (2026-11-26e).
# 2026-11-26 | n/a | Instance OCID, region and workspace key read from the config file's
#   job_details block (lake_ocid, region, workspace_key) when the parameters are blank.
#   New WORKSPACE_KEY parameter. Workspace precedence: WORKSPACE_KEY, WORKSPACE_NAME,
#   job_details.workspace_key, then all workspaces. Tag: n/a (2026-11-26b).
# 2026-11-26 | n/a | Cluster state compared case-insensitively ("Active" or "ACTIVE");
#   ALLOW_ACTIVE_SESSIONS literal fixed (Python True, not TRUE).
# 2026-11-26 | n/a | Created.
# ============================================================================================
#
# ACTIONS
#   STATUS          list the clusters in the instance with state, cores, memory, sessions.
#   RESTART_TIMING  restart the target cluster RUNS times; time each restart until ACTIVE.
#                   The target must be ACTIVE (AIDP restarts only a running cluster).
#   START_TIMING    start a STOPPED target and time it until ACTIVE (one run).
#   START / STOP / RESTART   one action, waits until the cluster settles.
#
# APIs used (https://aidp.<region>.oci.oraclecloud.com/api/20260430/...):
#   GET  aiDataPlatforms/{id}/workspaces                               list workspaces
#   GET  aiDataPlatforms/{id}/workspaces/{ws}/clusters                 list clusters
#   GET  aiDataPlatforms/{id}/workspaces/{ws}/clusters/{key}           cluster state
#   POST aiDataPlatforms/{id}/workspaces/{ws}/clusters/{key}/actions/{restart|start|stop}
#
# Requirements
#   - oci and requests on the cluster that runs this notebook (already used by the upload script).
#   - The service account in oci_config must be allowed to manage the target cluster.
#   - Never point TARGET_CLUSTER_NAME at the cluster this notebook runs on.
# ============================================================================================
import csv
import json
import os
import statistics
import time
import uuid
from datetime import datetime, timezone

import oci
import requests

# ------------------------------------------------------------------------------- parameters
CONFIG_FILE_PATH = "/Volumes/dev_plexus/default/workflow_config_files/aidp_cluster_control_config.json"  # n/a (2026-11-26c)

# OCID of the AIDP (Data Lake) instance. Leave "" to read job_details.lake_ocid       # n/a (2026-11-26b)
# (or aidp_id) from the config file.                                                  # n/a (2026-11-26b)
AIDP_ID = ""

ACTION = "STATUS"                 # STATUS | RESTART_TIMING | START_TIMING | START | STOP | RESTART
TARGET_CLUSTER_NAME = "PLEXUS_DEV_GEN_CLST_001"
WORKSPACE_KEY = ""                        # n/a (2026-11-26b) "" = WORKSPACE_NAME, else job_details.workspace_key
WORKSPACE_NAME = "dsor_plexus_dev_wkspc_002"      # "" = use job_details.workspace_key, else search all
RUNS = 1
POLL_SECONDS = 10
PAUSE_SECONDS_BETWEEN_RUNS = 60
TIMEOUT_MINUTES = 30
ALLOW_ACTIVE_SESSIONS = True             # n/a (2026-11-26) True = restart/stop even if notebooks are running on it
OUTPUT_CSV_PATH = ""                      # e.g. "/Volumes/dev_plexus/default/<volume>/cluster_restart_times.csv"

# Leave "" for the documented endpoint https://aidp.<region>.oci.oraclecloud.com/api.
# If that host is not reachable from your network, try the Workbench host, e.g.
# "https://9xxi4q3bvundxcrhww0phx.datalake.oci.oraclecloud.com/api".
API_BASE_OVERRIDE = ""
API_VERSION = "20260430"

TRANSITION_STATES = {"RESTARTING", "STOPPING", "STOPPED", "STARTING", "UPDATING", "ACCEPTED"}


# ------------------------------------------------------------------------------- helpers
def now_utc():
    return datetime.now(timezone.utc)


def norm_state(obj):
    """Upper-case the cluster state so "Active" and "ACTIVE" compare equal."""  # n/a (2026-11-26)
    if isinstance(obj, dict) and obj.get("state") is not None:
        obj["state"] = str(obj["state"]).strip().upper()
    return obj


def read_config(path):
    with open(path, "r") as f:
        return json.load(f)


def build_signer(oci_config):
    key_file = oci_config.get("key_file", "")                                            # n/a (2026-11-26c)
    if not oci_config.get("key_content") and not os.path.exists(key_file):               # n/a (2026-11-26e)
        # Not found as given: look for the same file name next to the config file.
        beside_config = os.path.join(os.path.dirname(CONFIG_FILE_PATH), os.path.basename(key_file))
        if os.path.exists(beside_config):
            print(f"Key file {key_file} not found; using {beside_config}")
            key_file = oci_config["key_file"] = beside_config
    if not oci_config.get("key_content") and not os.path.exists(key_file):               # n/a (2026-11-26d) path used as given
        folder = os.path.dirname(os.path.abspath(key_file))
        try:
            listing = sorted(os.listdir(folder))[:20]
        except Exception as exc:
            listing = f"folder not readable: {exc}"
        raise FileNotFoundError(f"Private key not found: {key_file} (resolved {os.path.abspath(key_file)}, "
                                f"cwd {os.getcwd()}). Files in that folder: {listing}")   # n/a (2026-11-26d)
    oci.config.validate_config(oci_config)
    try:                                                                                 # n/a (2026-11-26c)
        u = oci.identity.IdentityClient(oci_config).get_user(oci_config["user"]).data
        print(f"Signing as: {u.name} {u.email or ''}")
    except Exception as exc:  # the user may not be allowed to read IAM; not needed here
        print(f"Signing as user {oci_config['user'][:40]}... (name lookup skipped: {str(exc)[:80]})")
    kwargs = dict(tenancy=oci_config["tenancy"], user=oci_config["user"],
                  fingerprint=oci_config["fingerprint"], pass_phrase=oci_config.get("pass_phrase"))
    if oci_config.get("key_content"):
        kwargs["private_key_content"] = oci_config["key_content"]
    else:
        kwargs["private_key_file_location"] = oci_config["key_file"]
    return oci.signer.Signer(**kwargs)


class AidpApi:
    def __init__(self, base, aidp_id, signer):
        self.root = f"{base.rstrip('/')}/{API_VERSION}/aiDataPlatforms/{aidp_id}"
        self.signer = signer

    def _call(self, method, path, params=None, body=None, ok=(200,)):
        headers = {"opc-request-id": uuid.uuid4().hex}
        if method == "POST":
            headers["opc-retry-token"] = uuid.uuid4().hex
        r = requests.request(method, f"{self.root}{path}", params=params, json=body,
                             auth=self.signer, headers=headers, timeout=60)
        if r.status_code not in ok:
            raise RuntimeError(f"{method} {path} -> HTTP {r.status_code}: {r.text[:500]} "
                               f"(opc-request-id={r.headers.get('opc-request-id')})")
        return r

    def list_all(self, path, params=None):
        items, params = [], dict(params or {}, limit=100)
        while True:
            r = self._call("GET", path, params=params)
            items += r.json().get("items", [])
            page = r.headers.get("opc-next-page")
            if not page:
                return items
            params["page"] = page

    def workspace(self, ws_key):  # n/a (2026-11-26b)
        try:
            return self._call("GET", f"/workspaces/{ws_key}").json()
        except RuntimeError as exc:
            print(f"  workspace {ws_key} details not readable ({str(exc)[:120]}); using the key")
            return {"key": ws_key, "displayName": ws_key}

    def workspaces(self, name=""):
        return self.list_all("/workspaces", {"displayName": name} if name else None)

    def clusters(self, ws_key, name=""):
        return [norm_state(c) for c in  # n/a (2026-11-26)
                self.list_all(f"/workspaces/{ws_key}/clusters", {"displayName": name} if name else None)]

    def cluster(self, ws_key, key):
        return norm_state(self._call("GET", f"/workspaces/{ws_key}/clusters/{key}").json())  # n/a (2026-11-26)

    def action(self, ws_key, key, verb):
        r = self._call("POST", f"/workspaces/{ws_key}/clusters/{key}/actions/{verb}", body={}, ok=(200, 202))
        return r.headers.get("aidp-async-operation-key", ""), r.headers.get("opc-request-id", "")


def resolve_workspaces(api, ws_key, ws_name, config_ws_key):  # n/a (2026-11-26b)
    """Workspaces to search: WORKSPACE_KEY, WORKSPACE_NAME, job_details.workspace_key, or all."""
    if ws_key:
        return [api.workspace(ws_key)]
    if ws_name:
        found = api.workspaces(ws_name)
        if not found:
            raise ValueError(f"Workspace '{ws_name}' not found; check WORKSPACE_NAME or set WORKSPACE_KEY")
        return found
    if config_ws_key:
        return [api.workspace(config_ws_key)]
    return api.workspaces()


def find_cluster(api, cluster_name, workspaces, workspace_label=""):  # n/a (2026-11-26b)
    hits = []
    for ws in workspaces:
        for c in api.clusters(ws["key"], cluster_name):
            if c.get("displayName") == cluster_name:
                hits.append((ws, c))
    if not hits:
        raise ValueError(f"Cluster '{cluster_name}' not found"
                         + (f" in workspace '{workspace_label}'" if workspace_label else ""))  # n/a (2026-11-26b)
    if len(hits) > 1:
        raise ValueError(f"Cluster '{cluster_name}' exists in several workspaces "
                         f"{[w['displayName'] for w, _ in hits]}; set WORKSPACE_NAME")
    return hits[0]


def active_sessions(summary):
    return [s.get("path") for s in (summary.get("attachedSessions") or [])
            if str(s.get("state") or "").upper() == "ACTIVE"]  # n/a (2026-11-26)


def wait_for(api, ws_key, key, wanted, timeout_s, started, trail):
    """Poll until state is in `wanted`. Returns the time it was seen."""
    deadline = time.monotonic() + timeout_s
    last = trail[-1].split("@")[0] if trail else None
    while True:
        c = api.cluster(ws_key, key)
        state = c.get("state")
        if state != last:
            secs = (now_utc() - started).total_seconds()
            trail.append(f"{state}@{secs:.0f}s")
            print(f"    {now_utc():%H:%M:%S} state={state} (+{secs:.0f}s) {c.get('stateDetails') or ''}".rstrip())
            last = state
        if state in wanted:
            return now_utc()
        if state == "FAILED":
            raise RuntimeError(f"Cluster FAILED: {c.get('stateDetails')}")
        if time.monotonic() > deadline:
            raise TimeoutError(f"Not {sorted(wanted)} after {timeout_s}s (last state {state})")
        time.sleep(POLL_SECONDS)


def timed_action(api, ws, cluster, verb, run_no, start_state):
    """Send start/stop/restart and time it until the cluster settles."""
    ws_key, key = ws["key"], cluster["key"]
    final = {"STOPPED"} if verb == "stop" else {"ACTIVE"}
    trail = []
    requested = now_utc()
    op_key, req_id = api.action(ws_key, key, verb)
    print(f"  Run {run_no}: {verb} accepted {requested:%H:%M:%S} UTC (async op {op_key})")
    try:   # the state must leave its starting value first, or a fast poll sees the old state
        wait_for(api, ws_key, key, TRANSITION_STATES - final - {start_state}, 180, requested, trail)
    except TimeoutError:
        trail.append("NO_TRANSITION_SEEN")
        print("    no transition state seen within 180s")
    done = wait_for(api, ws_key, key, final, TIMEOUT_MINUTES * 60, requested, trail)
    secs = (done - requested).total_seconds()
    print(f"    {verb} finished in {secs:.0f}s ({secs / 60:.1f} min)")
    return {"run": run_no, "action": verb, "workspace": ws.get("displayName"),
            "cluster_name": cluster.get("displayName"), "cluster_key": key,
            "requested_utc": requested.isoformat(timespec="seconds"),
            "finished_utc": done.isoformat(timespec="seconds"),
            "seconds": round(secs), "minutes": round(secs / 60, 2),
            "state_sequence": " > ".join(trail), "async_operation_key": op_key,
            "opc_request_id": req_id, "poll_seconds": POLL_SECONDS}


def print_status(api, workspaces):  # n/a (2026-11-26b)
    print(f"{'Workspace':<28}{'Cluster':<30}{'State':<12}{'Cores':>7}{'Mem GB':>8}  Active sessions")
    for ws in workspaces:
        for c in api.clusters(ws["key"]):
            res = c.get("activeClusterResources") or {}
            print(f"{ws.get('displayName', '')[:27]:<28}{c.get('displayName', '')[:29]:<30}{c.get('state', ''):<12}"
                  f"{res.get('activeCores') or '-':>7}{res.get('activeMemoryInGBs') or '-':>8}  {len(active_sessions(c))}")


# ------------------------------------------------------------------------------- main
config = read_config(CONFIG_FILE_PATH)
oci_config = config["oci_config"]
job_details = config.get("job_details", {})                                            # n/a (2026-11-26b)
aidp_id = AIDP_ID or job_details.get("lake_ocid") or config.get("aidp_id", "")         # n/a (2026-11-26b)
if not aidp_id:
    raise ValueError("Set AIDP_ID or add job_details.lake_ocid to the config file")    # n/a (2026-11-26b)
region = job_details.get("region") or oci_config["region"]                              # n/a (2026-11-26b)
base = API_BASE_OVERRIDE or f"https://aidp.{region}.oci.oraclecloud.com/api"           # n/a (2026-11-26b)
api = AidpApi(base, aidp_id, build_signer(oci_config))
print(f"AIDP API: {api.root}")

ACTION = ACTION.upper()
workspaces = resolve_workspaces(api, WORKSPACE_KEY, WORKSPACE_NAME, job_details.get("workspace_key", ""))  # n/a (2026-11-26b)
print("Workspaces: " + ", ".join(f"{w.get('displayName')} ({w['key']})" for w in workspaces))         # n/a (2026-11-26b)
if ACTION == "STATUS":
    print_status(api, workspaces)                                                       # n/a (2026-11-26b)
else:
    ws, target = find_cluster(api, TARGET_CLUSTER_NAME, workspaces,
                              WORKSPACE_KEY or WORKSPACE_NAME or job_details.get("workspace_key", ""))  # n/a (2026-11-26b)
    state = api.cluster(ws["key"], target["key"]).get("state")
    print(f"Target: {target['displayName']} (workspace {ws['displayName']}, key {target['key']}) state={state}")

    busy = active_sessions(target)
    if busy and ACTION in ("RESTART_TIMING", "RESTART", "STOP") and not ALLOW_ACTIVE_SESSIONS:
        raise RuntimeError(f"{len(busy)} active session(s) on {TARGET_CLUSTER_NAME}: {busy[:5]}. "
                           "Wait for them to finish or set ALLOW_ACTIVE_SESSIONS = True.")

    plan = {"RESTART_TIMING": ("restart", "ACTIVE", RUNS), "RESTART": ("restart", "ACTIVE", 1),
            "START_TIMING": ("start", "STOPPED", 1), "START": ("start", "STOPPED", 1),
            "STOP": ("stop", "ACTIVE", 1)}
    if ACTION not in plan:
        raise ValueError(f"Unknown ACTION {ACTION}")
    verb, required_state, runs = plan[ACTION]
    if state != required_state:
        raise RuntimeError(f"{verb} needs the cluster {required_state}; it is {state}. "
                           + ("Use ACTION = 'START_TIMING' first." if verb == "restart" else ""))

    results = []
    for run in range(1, runs + 1):
        results.append(timed_action(api, ws, target, verb, run, required_state))
        if run < runs:
            time.sleep(PAUSE_SECONDS_BETWEEN_RUNS)

    secs = [r["seconds"] for r in results]
    print(f"\n{TARGET_CLUSTER_NAME} {verb}: runs={len(secs)} average={statistics.mean(secs):.0f}s "
          f"({statistics.mean(secs) / 60:.1f} min) min={min(secs)}s max={max(secs)}s (+/- {POLL_SECONDS}s)")
    for r in results:
        print(f"  run {r['run']}: {r['seconds']}s  {r['state_sequence']}")

    if OUTPUT_CSV_PATH:
        new_file = not os.path.exists(OUTPUT_CSV_PATH)
        with open(OUTPUT_CSV_PATH, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            if new_file:
                w.writeheader()
            w.writerows(results)
        print(f"Appended {len(results)} row(s) to {OUTPUT_CSV_PATH}")
