#!/usr/bin/env python3
"""
Browser front end for recon.py.

  python app.py                                   # http://127.0.0.1:8080, only this PC
  python app.py --host 0.0.0.0 --port 8080        # for the team (needs UI_USERS in .env)

What it does
  - Dashboard of every table pair in recon_config.yaml with its latest status
  - "Test connections" (recon.py --check) and "Run" for selected tables, from the browser
  - Live log while a run is going, with a Stop button; one run at a time
  - Run history (who started what, when) and searchable results per run, with downloads

Logins (.env, next to recon_config.yaml)
  UI_USERS=ravindra:Pass1,amit:Pass2      can view and start runs
  UI_VIEWERS=ops:Pass3                    can only view
Without UI_USERS the app only accepts connections from this PC (127.0.0.1).
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import glob
import hmac
import json
import os
import re
import secrets
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit

import yaml
from flask import (Flask, Response, abort, g, jsonify, redirect, render_template, request,
                   send_from_directory, url_for)

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import recon  # noqa: E402  (only for load_env_file)

app = Flask(__name__, template_folder=os.path.join(HERE, "templates"),
            static_folder=os.path.join(HERE, "static"))

STATE = {
    "config": None,        # absolute path to recon_config.yaml
    "workdir": None,       # folder of the config: runs start here, paths resolve from here
    "users": {},           # name -> password (may run)
    "viewers": {},         # name -> password (view only)
    "auth": False,
    "allowed_hosts": set(),  # extra host names users type in the browser (UI_ALLOWED_HOSTS)
}
_jobs_lock = threading.Lock()
_procs: dict[str, subprocess.Popen] = {}
RUN_ID_RE = re.compile(r"^[0-9A-Za-z_]{6,40}$")


# =============================================================================
# Config / paths
# =============================================================================
def load_cfg() -> dict:
    with open(STATE["config"], encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def output_dir() -> str:
    out = (load_cfg().get("settings") or {}).get("output_dir", "recon_output")
    path = out if os.path.isabs(out) else os.path.join(STATE["workdir"], out)
    os.makedirs(path, exist_ok=True)
    return path


def table_list(cfg) -> list[dict]:
    rows = []
    for t in cfg.get("tables") or []:
        s, tg = t.get("source") or {}, t.get("target") or {}
        rows.append({
            "name": str(t.get("name")), "enabled": t.get("enabled", True),
            "source": f'{s.get("connection")} : {s.get("table")}',
            "target": f'{tg.get("connection")} : {tg.get("table")}',
            "partition": ((t.get("partition") or {}).get("column") or "-"),
            "columns": len(t.get("columns") or {}),
        })
    return rows


# =============================================================================
# Jobs (each browser-started run / check), stored in output_dir/ui_jobs.json
# =============================================================================
def _jobs_file():
    return os.path.join(output_dir(), "ui_jobs.json")


def read_jobs() -> list[dict]:
    try:
        with open(_jobs_file(), encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_jobs(jobs):
    tmp = _jobs_file() + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(jobs[-500:], f, indent=1)
    os.replace(tmp, _jobs_file())


def update_job(job_id, **changes):
    with _jobs_lock:
        jobs = read_jobs()
        for j in jobs:
            if j["id"] == job_id:
                j.update(changes)
        save_jobs(jobs)


def get_job(job_id):
    return next((j for j in read_jobs() if j["id"] == job_id), None)


def running_job():
    return next((j for j in reversed(read_jobs()) if j["status"] == "RUNNING"), None)


def log_path(job_id):
    d = os.path.join(output_dir(), "ui_logs")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, f"{job_id}.log")


def start_job(kind, user, tables=None, level=None):
    """Start recon.py in its own process. Returns (job, error_message)."""
    with _jobs_lock:
        jobs = read_jobs()
        busy = next((j for j in jobs if j["status"] == "RUNNING"), None)
        if busy:
            return None, (f"A {busy['kind']} started by {busy['user']} at "
                          f"{busy['started'][11:16]} is still running. Wait for it or stop it.")
        job_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + secrets.token_hex(2)
        cmd = [sys.executable, "-u", os.path.join(HERE, "recon.py"), "-c", STATE["config"]]
        if kind == "check":
            cmd.append("--check")
        else:
            cmd += ["--run-id", job_id, "-l", str(level)]
            for t in tables:
                cmd += ["-t", t]
        job = {"id": job_id, "kind": kind, "user": user, "tables": tables or [], "level": level,
               "started": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "ended": None,
               "status": "RUNNING", "exit_code": None}
        jobs.append(job)
        save_jobs(jobs)

    logf = open(log_path(job_id), "w", encoding="utf-8")
    logf.write(f"Started by {user}: {' '.join(cmd[2:])}\n\n")
    logf.flush()
    env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
    try:
        proc = subprocess.Popen(cmd, stdout=logf, stderr=subprocess.STDOUT, cwd=STATE["workdir"],
                                env=env)
    except Exception as e:
        logf.write(f"Could not start: {e}\n")
        logf.close()
        update_job(job_id, status="ERROR", ended=dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return None, f"Could not start recon.py: {e}"
    _procs[job_id] = proc

    def wait():
        code = proc.wait()
        logf.close()
        stopped = getattr(proc, "_stopped_by", None)
        if stopped:
            status = "STOPPED"
        elif kind == "check":
            status = "OK" if code == 0 else "FAILED"
        else:
            status = {0: "MATCHED", 1: "MISMATCH"}.get(code, "ERROR")
        update_job(job_id, status=status, exit_code=code, stopped_by=stopped,
                   ended=dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        _procs.pop(job_id, None)

    threading.Thread(target=wait, daemon=True).start()
    return job, None


def tail(path, max_lines=600):
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return ""
    return "".join(lines[-max_lines:])


# =============================================================================
# Run results (recon_<run_id>.json written by recon.py)
# =============================================================================
def run_files(limit=300):
    files = glob.glob(os.path.join(output_dir(), "recon_*.json"))
    files.sort(key=os.path.getmtime, reverse=True)
    return files[:limit]


def load_run(run_id):
    if not RUN_ID_RE.match(run_id):
        return None
    try:
        with open(os.path.join(output_dir(), f"recon_{run_id}.json"), encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def overall(statuses):
    if not statuses:
        return "-"
    if "ERROR" in statuses:
        return "ERROR"
    return "MISMATCH" if "MISMATCH" in statuses else "MATCHED"


def runs_overview(limit=300):
    jobs = {j["id"]: j for j in read_jobs()}
    out = []
    for f in run_files(limit):
        try:
            with open(f, encoding="utf-8") as fh:
                d = json.load(fh)
        except (OSError, json.JSONDecodeError):
            continue
        st = [s["status"] for s in d["summary"]]
        job = jobs.get(d["run_id"])
        out.append({
            "run_id": d["run_id"], "run_time": d["run_time"], "level": d.get("level"),
            "user": job["user"] if job else "command line / schedule",
            "tables": len(st), "status": overall(st),
            "matched": st.count("MATCHED"), "mismatched": st.count("MISMATCH"),
            "errors": st.count("ERROR"),
            "seconds": max((s.get("seconds") or 0 for s in d["summary"]), default=0),
            "summary": d["summary"]})
    return out


def latest_per_table(runs):
    latest = {}
    for r in runs:                       # newest first
        for s in r["summary"]:
            latest.setdefault(s["table"], {**s, "run_id": r["run_id"], "run_time": r["run_time"]})
    return latest


# =============================================================================
# Security: login (HTTP Basic) and same-origin check on every POST
# =============================================================================
def _parse_users(value):
    users = {}
    for part in (value or "").split(","):
        if ":" in part:
            u, p = part.split(":", 1)
            if u.strip() and p.strip():
                users[u.strip()] = p.strip()
    return users


LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "[::1]"}
_failed: dict[str, list[float]] = {}          # ip -> times of failed logins
_failed_lock = threading.Lock()
MAX_FAILS, FAIL_WINDOW = 10, 15 * 60          # 10 wrong passwords per 15 min per IP


def _host_allowed() -> bool:
    """Blocks DNS-rebinding: a page on another site that points its own name at this
    server. Only names we expect may appear in the Host header."""
    host = request.host.rsplit(":", 1)[0] if not request.host.startswith("[") \
        else request.host.split("]")[0] + "]"
    host = host.lower()
    if host in LOCAL_HOSTS:
        return True
    if not STATE["auth"]:
        return False                          # no logins -> this PC only
    allowed = STATE["allowed_hosts"]
    return not allowed or host in allowed


def _too_many_failures(ip) -> bool:
    now = time.time()
    with _failed_lock:
        recent = [t for t in _failed.get(ip, []) if now - t < FAIL_WINDOW]
        _failed[ip] = recent
        return len(recent) >= MAX_FAILS


def _record_failure(ip):
    with _failed_lock:
        _failed.setdefault(ip, []).append(time.time())


@app.before_request
def guard():
    if not _host_allowed():
        return Response("Unknown host name. Add it to UI_ALLOWED_HOSTS in .env if it is yours.",
                        400)
    if request.endpoint == "static":
        return None
    if STATE["auth"]:
        ip = request.remote_addr or "?"
        if _too_many_failures(ip):
            return Response("Too many wrong passwords from this computer. Try again in 15 "
                            "minutes.", 429)
        hdr = request.headers.get("Authorization", "")
        user, ok, can_run = None, False, False
        if hdr.startswith("Basic "):
            try:
                u, p = base64.b64decode(hdr[6:]).decode("utf-8").split(":", 1)
            except Exception:
                u, p = "", ""
            for store, runner in ((STATE["users"], True), (STATE["viewers"], False)):
                if u in store and hmac.compare_digest(store[u], p):
                    user, ok, can_run = u, True, runner
        if not ok:
            if hdr:
                _record_failure(ip)
            return Response("Login required", 401,
                            {"WWW-Authenticate": 'Basic realm="Recon", charset="UTF-8"'})
        g.user, g.can_run = user, can_run
    else:
        g.user, g.can_run = "local", True
    if request.method == "POST":
        src = request.headers.get("Origin") or request.headers.get("Referer") or ""
        if urlsplit(src).netloc != request.host:
            abort(403, "Request blocked: it did not come from this app's own pages.")
        if not g.can_run:
            abort(403, "Your login can view results but not start or stop runs.")
    return None


@app.after_request
def security_headers(resp):
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "same-origin")
    resp.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; "
        "form-action 'self'")
    if request.endpoint != "static":
        resp.headers["Cache-Control"] = "no-store"   # pages contain business data
    return resp


@app.context_processor
def common():
    return {"user": getattr(g, "user", None), "can_run": getattr(g, "can_run", False),
            "busy": running_job(), "config_path": STATE["config"]}


# =============================================================================
# Pages
# =============================================================================
@app.get("/")
def dashboard():
    cfg = load_cfg()
    tables = table_list(cfg)
    runs = runs_overview()
    latest = latest_per_table(runs)
    for t in tables:
        t["last"] = latest.get(t["name"])
    counts = {"MATCHED": 0, "MISMATCH": 0, "ERROR": 0, "never": 0}
    for t in tables:
        counts[t["last"]["status"] if t["last"] else "never"] += 1
    jobs = list(reversed(read_jobs()))[:8]
    last_check = next((j for j in reversed(read_jobs()) if j["kind"] == "check"), None)
    return render_template("dashboard.html", tables=tables, counts=counts, jobs=jobs,
                           last_run=runs[0] if runs else None, last_check=last_check,
                           level=(cfg.get("settings") or {}).get("level", 3),
                           msg=request.args.get("msg"))


@app.post("/run")
def run():
    names = {t["name"] for t in table_list(load_cfg())}
    picked = [t for t in request.form.getlist("tables") if t in names]
    if not picked:
        return redirect(url_for("dashboard", msg="Select at least one table to run."))
    level = request.form.get("level", "3")
    level = int(level) if level in ("1", "2", "3") else 3
    job, err = start_job("run", g.user, picked, level)
    if err:
        return redirect(url_for("dashboard", msg=err))
    return redirect(url_for("job_page", job_id=job["id"]))


@app.post("/check")
def check():
    job, err = start_job("check", g.user)
    if err:
        return redirect(url_for("dashboard", msg=err))
    return redirect(url_for("job_page", job_id=job["id"]))


@app.get("/jobs/<job_id>")
def job_page(job_id):
    job = get_job(job_id)
    if not job:
        abort(404)
    return render_template("job.html", job=job, log=tail(log_path(job_id)),
                           has_results=load_run(job_id) is not None)


@app.get("/api/jobs/<job_id>")
def job_api(job_id):
    job = get_job(job_id)
    if not job:
        abort(404)
    return jsonify({**job, "log": tail(log_path(job_id)), "duration": dur(job),
                    "has_results": job["kind"] == "run" and load_run(job_id) is not None})


@app.post("/jobs/<job_id>/stop")
def job_stop(job_id):
    proc = _procs.get(job_id)
    if proc and proc.poll() is None:
        proc._stopped_by = g.user
        proc.terminate()
        with open(log_path(job_id), "a", encoding="utf-8") as f:
            f.write(f"\n*** Stopped by {g.user} ***\n")
    return redirect(url_for("job_page", job_id=job_id))


@app.get("/runs")
def runs_page():
    return render_template("runs.html", runs=runs_overview(),
                           jobs=[j for j in reversed(read_jobs()) if j["kind"] == "check"][:20])


@app.get("/runs/<run_id>")
def run_detail(run_id):
    d = load_run(run_id)
    if not d:
        abort(404)
    job = get_job(run_id)
    files = {ext: os.path.exists(os.path.join(output_dir(), f"recon_{run_id}.{ext}"))
             for ext in ("xlsx", "html")}
    st = [s["status"] for s in d["summary"]]
    bad_parts = [p for p in d["partitions"] if p["status"] != "MATCHED"]
    return render_template("run_detail.html", d=d, job=job, files=files, status=overall(st),
                           bad_parts=bad_parts,
                           issues=sorted({x["issue"] for x in d["differences"]}),
                           table_names=[s["table"] for s in d["summary"]])


@app.get("/download/<name>")
def download(name):
    if not (re.fullmatch(r"recon_[0-9A-Za-z_]{6,40}\.(xlsx|html)", name)
            or name == "recon_history.csv"):
        abort(404)
    return send_from_directory(output_dir(), name, as_attachment=True)


@app.get("/config")
def config_page():
    cfg = load_cfg()
    return render_template("config.html", tables=table_list(cfg),
                           connections={n: {k: v for k, v in c.items()
                                            if "pass" not in k.lower()}
                                        for n, c in (cfg.get("connections") or {}).items()})


@app.errorhandler(403)
def forbidden(e):
    return render_template("message.html", title="Not allowed", text=e.description), 403


@app.errorhandler(404)
def not_found(e):
    return render_template("message.html", title="Not found",
                           text="That page or run does not exist."), 404


@app.template_filter("dur")
def dur(job):
    try:
        a = dt.datetime.fromisoformat(job["started"])
        b = dt.datetime.fromisoformat(job["ended"]) if job.get("ended") else dt.datetime.now()
        s = int((b - a).total_seconds())
        return f"{s // 60}m {s % 60:02d}s" if s >= 60 else f"{s}s"
    except Exception:
        return "-"


@app.template_filter("checks")
def checks_short(value):
    """'rows, sum(PREMIUM), ...' -> a short readable summary (full list goes in a tooltip)."""
    items = [c.strip() for c in (value or "").split(",") if c.strip()]
    if not items:
        return ""
    if "rows" in items:
        rest = len(items) - 1
        return "Row count differs" + (f" (+{rest} other checks)" if rest else "")
    special = {"spot_check_rows": "Found by random spot check",
               "duplicate_keys_in_target": "Duplicate keys in target",
               "distinct_keys": "Distinct key count differs"}
    notes = [special[c] for c in items if c in special]
    items = [c for c in items if c not in special]
    if not items:
        return "; ".join(notes)
    cols = []
    for c in items:
        col = c[c.find("(") + 1:-1] if "(" in c else c
        if col not in cols:
            cols.append(col)
    shown = ", ".join(cols[:3]) + (f" +{len(cols) - 3} more" if len(cols) > 3 else "")
    return "; ".join([f"Values differ in {shown}"] + notes)


@app.template_filter("num")
def num(v):
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return "" if v is None else v


# =============================================================================
# Start
# =============================================================================
def main(argv=None):
    ap = argparse.ArgumentParser(description="Web front end for the recon tool")
    ap.add_argument("-c", "--config", default=os.path.join(HERE, "recon_config.yaml"))
    ap.add_argument("--host", default="127.0.0.1",
                    help="127.0.0.1 = this PC only; 0.0.0.0 = reachable by the team")
    ap.add_argument("--port", type=int, default=8080)
    a = ap.parse_args(argv)

    STATE["config"] = os.path.abspath(a.config)
    STATE["workdir"] = os.path.dirname(STATE["config"])
    if not os.path.exists(STATE["config"]):
        sys.exit(f"Config not found: {STATE['config']}")
    recon.load_env_file(os.path.join(STATE["workdir"], ".env"))
    STATE["users"] = _parse_users(os.environ.get("UI_USERS"))
    STATE["viewers"] = _parse_users(os.environ.get("UI_VIEWERS"))
    STATE["auth"] = bool(STATE["users"] or STATE["viewers"])
    STATE["allowed_hosts"] = {h.strip().lower() for h in
                              os.environ.get("UI_ALLOWED_HOSTS", "").split(",") if h.strip()}
    local_only = a.host in ("127.0.0.1", "localhost", "::1")
    if not local_only and not STATE["auth"]:
        sys.exit("Refusing to open the app to the network without logins.\n"
                 "Add  UI_USERS=yourname:yourpassword  to .env (next to recon_config.yaml), "
                 "or start with --host 127.0.0.1")

    # A previous app process may have died mid-run: don't leave jobs stuck on RUNNING
    with _jobs_lock:
        jobs = read_jobs()
        for j in jobs:
            if j["status"] == "RUNNING":
                j["status"], j["ended"] = "INTERRUPTED", j.get("ended") or j["started"]
        save_jobs(jobs)

    shown = "localhost" if local_only else a.host
    print(f"\nRecon web app on  http://{shown}:{a.port}\n"
          f"Config: {STATE['config']}\n"
          f"Logins: {'on (' + str(len(STATE['users'])) + ' run, ' + str(len(STATE['viewers'])) + ' view)' if STATE['auth'] else 'off (this PC only)'}\n")
    try:
        from waitress import serve
        serve(app, host=a.host, port=a.port, threads=8)
    except ImportError:
        app.run(host=a.host, port=a.port, threaded=True)


if __name__ == "__main__":
    main()