import os
import queue
import sqlite3
import subprocess
import threading
import shutil
from pathlib import Path
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
from typing import Any

from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from fastapi import UploadFile, File, Form
from fastapi import HTTPException

app = FastAPI(title="Mission Control Dashboard")
STATIC_DIR = Path("/home/michael/dashboard/static")
DASHBOARD_DIR = Path(__file__).resolve().parent
SUBJECTS_DIR = Path("/home/michael/.hermes/subjects")
PROFILES_DIR = Path("/home/michael/.hermes/profiles")
DB_PATH = Path(os.environ.get("AGENT_LOG_DB", "/home/michael/.hermes/agent-logs.db"))
HERMES_VENV = Path("/home/michael/.hermes/hermes-agent/venv/bin/python")
HERMES_ROOT = "/home/michael/.hermes"


def run_agent(agent: str, task: str) -> dict:
    profile_dir = PROFILES_DIR / agent if agent != "bill" else Path(HERMES_ROOT)
    if not profile_dir.exists():
        raise ValueError(f"unknown agent: {agent}")

    env = os.environ.copy()
    env["HERMES_HOME"] = str(profile_dir)
    env["AGENT_LOG_DB"] = str(DB_PATH)
    env["PYTHONUNBUFFERED"] = "1"

    cmd = [
        str(HERMES_VENV),
        "-m",
        "hermes_cli.main",
        "-z",
        task,
        "--yolo",
        "--accept-hooks",
    ]
    result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
        cwd=str(profile_dir),
    )
    return {
        "agent": agent,
        "task": task,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


@app.get("/api/version")
def api_version():
    return {"version": "0.1.0", "service": "mission-control-dashboard"}


@app.get("/api/subjects")
def list_subjects():
    if not SUBJECTS_DIR.exists():
        return {"subjects": []}
    subjects = [child.name for child in sorted(SUBJECTS_DIR.iterdir()) if child.is_dir()]
    return {"subjects": subjects}


@app.post("/api/agent/run")
def agent_run(payload: dict):
    agent = payload.get("agent")
    task = payload.get("task")
    if not agent or not task:
        return {"error": "agent and task are required"}
    try:
        result = run_agent(agent, task)
        return result
    except Exception as exc:
        return {"error": str(exc)}


def _sse_event(data: dict) -> str:
    import json

    return "data: " + json.dumps(data) + "\n\n"


@app.post("/api/agent/stream")
def agent_stream(payload: dict):
    agent = payload.get("agent")
    task = payload.get("task")
    if not agent or not task:
        return StreamingResponse(
            iter([_sse_event({"status": "error", "message": "agent and task are required"})]),
            media_type="text/event-stream",
        )

    q: "queue.Queue[dict | None]" = queue.Queue()

    def worker():
        try:
            q.put({"status": "starting", "message": "Agent starting..."})
            result = run_agent(agent, task)
            q.put({"status": "complete", "result": result.get("stdout", result.get("stderr", ""))})
        except Exception as exc:
            q.put({"status": "error", "message": str(exc)})
        finally:
            q.put(None)

    threading.Thread(target=worker, daemon=True).start()

    def event_generator():
        while True:
            item = q.get()
            if item is None:
                break
            yield _sse_event(item)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/activity")
def recent_activity(limit: int = 50):
    if not DB_PATH.exists():
        return []
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT agent_name, task_description, status, model_used, created_at FROM agent_tasks ORDER BY datetime(created_at) DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/profiles")
def list_profiles():
    if not PROFILES_DIR.exists():
        return {"profiles": []}
    profiles = []
    for child in sorted(PROFILES_DIR.iterdir()):
        if child.is_dir() and (child / "SOUL.md").exists():
            profiles.append({
                "name": child.name,
                "path": str(child),
            })
    return {"profiles": profiles}


STATIC_DIR.mkdir(parents=True, exist_ok=True)

MANUAL_TASKS_DB = Path("/home/michael/.hermes/manual_tasks.db")

# ---------------------------------------------------------------------------
# Manual tasks (homework tracking)
# ---------------------------------------------------------------------------

def _get_tasks_conn():
    conn = sqlite3.connect(str(MANUAL_TASKS_DB))
    conn.row_factory = sqlite3.Row
    return conn


def _init_manual_tasks_db():
    MANUAL_TASKS_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = _get_tasks_conn()
    try:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS manual_tasks (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   subject TEXT NOT NULL,
                   title TEXT NOT NULL,
                   task_type TEXT NOT NULL DEFAULT 'Opgave',
                   due_date TEXT,
                   notes TEXT DEFAULT '',
                   completed INTEGER NOT NULL DEFAULT 0,
                   created_at TEXT NOT NULL
               )"""
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(manual_tasks)").fetchall()}
        if "due_time" not in columns:
            conn.execute("ALTER TABLE manual_tasks ADD COLUMN due_time TEXT")
        conn.commit()
    finally:
        conn.close()


def _init_backup_repo():
    try:
        git_dir = SUBJECTS_DIR / ".git"
        if not git_dir.exists():
            subprocess.run(["git", "init", str(SUBJECTS_DIR)], check=True, capture_output=True)
            subprocess.run(["git", "config", "user.email", "backup@localhost"], cwd=str(SUBJECTS_DIR), check=True, capture_output=True)
            subprocess.run(["git", "config", "user.name", "Mission Control Backup"], cwd=str(SUBJECTS_DIR), check=True, capture_output=True)
            ig = SUBJECTS_DIR / ".gitignore"
            ig.write_text("*/uploads/\n", encoding="utf-8")
            subprocess.run(["git", "add", "-A"], cwd=str(SUBJECTS_DIR), check=True, capture_output=True)
            subprocess.run(["git", "commit", "-m", "initial backup", "--allow-empty"], cwd=str(SUBJECTS_DIR), check=True, capture_output=True)
    except Exception:
        # NEVER crash startup because of missing git or other backup setup issues
        pass


def backup_subjects(message: str) -> dict:
    try:
        if MANUAL_TASKS_DB.exists():
            shutil.copy2(str(MANUAL_TASKS_DB), str(SUBJECTS_DIR / "_manual_tasks_backup.db"))
        subprocess.run(["git", "add", "-A"], cwd=str(SUBJECTS_DIR), check=True, capture_output=True)
        subprocess.run(["git", "commit", "-m", message, "--allow-empty"], cwd=str(SUBJECTS_DIR), check=True, capture_output=True)
        res = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=str(SUBJECTS_DIR), check=True, capture_output=True, text=True)
        h = res.stdout.strip()
        return {"commit": h, "message": message}
    except Exception as exc:
        return {"error": str(exc)}


def backup_dashboard(message: str) -> dict:
    try:
        subprocess.run(["git", "add", "-A"], cwd=str(DASHBOARD_DIR), check=True, capture_output=True)
        subprocess.run(["git", "commit", "-m", message, "--allow-empty"], cwd=str(DASHBOARD_DIR), check=True, capture_output=True)
        res = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(DASHBOARD_DIR), check=True, capture_output=True, text=True)
        h = res.stdout.strip()
        pushed = False
        try:
            subprocess.run(["git", "push"], cwd=str(DASHBOARD_DIR), check=True, capture_output=True)
            pushed = True
        except Exception:
            pushed = False
        return {"commit": h, "message": message, "pushed": pushed}
    except Exception as exc:
        return {"error": str(exc)}


# Initialise once at import / startup
_init_manual_tasks_db()
_init_backup_repo()


@app.get("/api/tasks")
def list_tasks(subject: str = "", include_completed: bool = True):
    conn = _get_tasks_conn()
    try:
        conditions = []
        params: list[Any] = []
        if subject:
            conditions.append("subject = ?")
            params.append(subject)
        if not include_completed:
            conditions.append("completed = 0")
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = (
            f"SELECT id, subject, title, task_type, due_date, due_time, notes, completed, created_at"
            f" FROM manual_tasks{where}"
            " ORDER BY CASE WHEN due_date IS NULL THEN 1 ELSE 0 END, due_date ASC, created_at ASC"
        )
        rows = conn.execute(sql, params).fetchall()
        return [
            {
                "id": r["id"],
                "subject": r["subject"],
                "title": r["title"],
                "task_type": r["task_type"],
                "due_date": r["due_date"],
                "due_time": r["due_time"],
                "notes": r["notes"],
                "completed": bool(r["completed"]),
                "created_at": r["created_at"],
            }
            for r in rows
        ]
    finally:
        conn.close()


@app.post("/api/tasks")
def create_task(payload: dict):
    if payload is None:
        raise HTTPException(status_code=415, detail="JSON body required")
    subject = (payload.get("subject") or "").strip()
    title = (payload.get("title") or "").strip()
    if not subject or not title:
        raise HTTPException(status_code=400, detail="subject and title are required")
    task_type = (payload.get("task_type") or "Opgave").strip()
    due_date = payload.get("due_date")
    notes = payload.get("notes", "")
    if not isinstance(notes, str):
        notes = str(notes)
    created_at = datetime.now(timezone.utc).isoformat()
    conn = _get_tasks_conn()
    try:
        cur = conn.execute(
            """INSERT INTO manual_tasks (subject, title, task_type, due_date, notes, completed, created_at)
               VALUES (?, ?, ?, ?, ?, 0, ?)""",
            (subject, title, task_type, due_date, notes, created_at),
        )
        conn.commit()
        task_id = cur.lastrowid
        row = conn.execute(
            "SELECT id, subject, title, task_type, due_date, notes, completed, created_at FROM manual_tasks WHERE id=?",
            (task_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=500, detail="failed to read created task")
        return {
            "id": row["id"],
            "subject": row["subject"],
            "title": row["title"],
            "task_type": row["task_type"],
            "due_date": row["due_date"],
            "notes": row["notes"],
            "completed": bool(row["completed"]),
            "created_at": row["created_at"],
        }
    finally:
        conn.close()


def _update_task(task_id: int, payload: dict):
    allowed = {"subject", "title", "task_type", "due_date", "notes", "completed"}
    updates = {k: payload[k] for k in payload.keys() if k in allowed}
    if not updates:
        return None
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [task_id]
    conn = _get_tasks_conn()
    try:
        conn.execute(f"UPDATE manual_tasks SET {set_clause} WHERE id=?", values)
        conn.commit()
        if conn.total_changes == 0:
            return None
        row = conn.execute(
            "SELECT id, subject, title, task_type, due_date, notes, completed, created_at FROM manual_tasks WHERE id=?",
            (task_id,),
        ).fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "subject": row["subject"],
            "title": row["title"],
            "task_type": row["task_type"],
            "due_date": row["due_date"],
            "notes": row["notes"],
            "completed": bool(row["completed"]),
            "created_at": row["created_at"],
        }
    finally:
        conn.close()


@app.put("/api/tasks/{task_id}")
def update_task(task_id: int, payload: dict):
    if payload is None:
        raise HTTPException(status_code=415, detail="JSON body required")
    if "completed" in payload:
        payload["completed"] = 1 if payload["completed"] else 0
    result = _update_task(task_id, payload)
    if result is None:
        raise HTTPException(status_code=404, detail="task not found")
    return result


@app.delete("/api/tasks/{task_id}")
def delete_task(task_id: int):
    conn = _get_tasks_conn()
    try:
        conn.execute("DELETE FROM manual_tasks WHERE id=?", (task_id,))
        conn.commit()
        if conn.total_changes == 0:
            raise HTTPException(status_code=404, detail="task not found")
        return {"message": "task deleted"}
    finally:
        conn.close()


@app.post("/api/upload")
def upload_file(file: UploadFile = File(...), subject: str = Form(...), session_name: str = Form(default="")):
    if not subject:
        raise HTTPException(status_code=400, detail="subject is required")
    subject_dir = SUBJECTS_DIR / subject / "uploads"
    subject_dir.mkdir(parents=True, exist_ok=True)
    filename = file.filename
    if session_name:
        note_name = "".join(c if (c.isalnum() or c in {"-", "_", " "}) else "_" for c in session_name.strip())
        note_name = note_name.replace(" ", "_")
        filename = note_name + Path(file.filename).suffix
    else:
        note_name = Path(file.filename).stem
    dest = subject_dir / filename
    dest.write_bytes(file.file.read())
    def pipeline():
        try:
            run_agent("vault", f"Log uploaded file: {dest} for subject {subject}")
            run_agent("scholar", f"Extract full notes from {dest} and save to /home/michael/.hermes/subjects/{subject}/notes/{note_name}.md")
            run_agent(
                "quizmaster",
                f"Generate a quiz from /home/michael/.hermes/subjects/{subject}/notes/{note_name}.md and save to /home/michael/.hermes/subjects/{subject}/quizzes/{note_name}_quiz.md",
            )
            backup_subjects(f"auto: upload completed for {subject}/{note_name}")
        except Exception:
            pass
    threading.Thread(target=pipeline, daemon=True).start()
    return {"message": "File received", "subject": subject, "filename": filename}


@app.get("/api/subjects/{subject}/files")
def list_subject_files(subject: str):
    base = SUBJECTS_DIR / subject
    if not base.exists():
        return {"files": []}
    files = []
    for p in sorted(base.rglob("*")):
        if p.is_file():
            files.append(str(p.relative_to(base)))
    return {"files": files}


@app.get("/api/subjects/{subject}/notes/{filename:path}")
def read_subject_note(subject: str, filename: str):
    path = SUBJECTS_DIR / subject / filename
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="file not found")
    try:
        content = path.read_text(errors="replace")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"content": content}


@app.delete("/api/subjects/{subject}")
def delete_subject(subject: str):
    subject_path = SUBJECTS_DIR / subject
    if not subject_path.exists() or not subject_path.is_dir():
        raise HTTPException(status_code=404, detail="subject not found")
    try:
        shutil.rmtree(subject_path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"message": "subject deleted"}


@app.delete("/api/subjects/{subject}/notes/{filename:path}")
def delete_subject_note(subject: str, filename: str):
    path = SUBJECTS_DIR / subject / filename
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="file not found")
    try:
        path.unlink()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"message": "note deleted"}


@app.put("/api/subjects/{subject}/notes/{filename:path}")
def overwrite_subject_note(subject: str, filename: str, payload: dict):
    if payload is None:
        raise HTTPException(status_code=415, detail="JSON body required")
    content = payload.get("content")
    if content is None:
        raise HTTPException(status_code=400, detail="'content' field is required")
    path = SUBJECTS_DIR / subject / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(content, encoding="utf-8")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"message": "note overwritten"}


@app.post("/api/subjects/{subject}/rename-note")
def rename_subject_note(payload: dict):
    if payload is None:
        raise HTTPException(status_code=415, detail="JSON body required")
    subject = payload.get("subject")
    old_filename = payload.get("old_filename")
    new_filename = payload.get("new_filename")
    if not subject or not old_filename or not new_filename:
        raise HTTPException(status_code=400, detail="subject, old_filename, and new_filename are required")
    old_path = SUBJECTS_DIR / subject / old_filename
    new_path = SUBJECTS_DIR / subject / new_filename
    if not old_path.exists() or not old_path.is_file():
        raise HTTPException(status_code=404, detail="old note not found")
    new_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        old_path.rename(new_path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"message": "note renamed"}


ICAL_URLS = [
    "https://cloud.timeedit.net/ucl/web/pub/ri6YZ80xy05Z1oQ6675W0m625Q7ZQ3Q5o6QZ650Q069153f6Z0m3uQDZ6Z541noQj1mm1902CtQ10093t12k29k526ldZ2AlF001A119CCCBAED4ECF5423A0066D.ics",
    "https://cloud.timeedit.net/ucl/web/pub/ri6YZ80xy05Z1oQ6675W0m625Q7ZQ3Q0o6QZ650Q069154f6Z0m4uQ5Z6Z541noQj1mm1902EtQB0093t18k99k526ldZ2ClC05921B8DDBC0404C101E8F0F89CB.ics",
    "https://cloud.timeedit.net/ucl/web/pub/ri6YZ85xy05Z1oQ6675W5m625Q7ZQ3Q5o6QZ650Q069150f6Z0m0uQ6Z6Z541noQj1mm19021tQ40093t13k09k526ldZ2Dl60D7E1C145C3469DED7B68DD721F.ics",
    "https://cloud.timeedit.net/ucl/web/pub/ri6YZ85xy05Z1oQ6675W5m625Q7ZQ3Q0o6QZ650Q069155f6Z0m5uQBZ6Z541noQj1mm19022tQ40093t16k69k526ldZ29l8000F1B3C36E96CF5C4391763264A.ics",
    "https://cloud.timeedit.net/ucl/web/pub/ri6YZ85xy05Z1oQ6675W5m625Q7ZQ3Q5o6QZ650Q069156f6Z0m6uQ3Z6Z541noQj1mm1902DtQ10093t1DkD9k526ldZ22l1003716CAD4148A2E5CF55A4A111.ics",
]
CPH = ZoneInfo("Europe/Copenhagen")
_now = datetime.now(timezone.utc)


def _to_dt(value):
    if value is None:
        return None
    dt = value.dt
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    if isinstance(dt, date):
        return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    return None


def _attendee_cn(attendee_value):
    if not attendee_value:
        return ""
    attendee = attendee_value
    if hasattr(attendee_value, "rep_param"):
        attendee = attendee_value
    cn = attendee.params.get("CN")
    if cn is None and hasattr(attendee_value, "address"):
        # sometimes library stores raw
        return ""
    return str(cn) if cn else ""


def _parse_ical(text: str):
    try:
        import icalendar
    except Exception as exc:
        raise RuntimeError(f"icalendar import failed: {exc}")
    cal = icalendar.Calendar.from_ical(text)
    return [
        {
            "uid": str(comp.get("UID", "")),
            "title": str(comp.get("SUMMARY", "")),
            "start": _to_dt(comp.get("DTSTART")),
            "end": _to_dt(comp.get("DTEND") or comp.get("DURATION")),
            "location": str(comp.get("LOCATION", "") or ""),
            "subject": _attendee_cn(comp.get("ATTENDEE")),
        }
        for comp in cal.walk()
        if comp.name == "VEVENT"
    ]


def _fetch_ical(url: str):
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return _parse_ical(resp.text)
    except Exception as exc:
        print(f"schedule fetch error {url}: {exc}")
        return []


@app.get("/api/schedule")
def schedule():
    events = []
    seen = set()
    with ThreadPoolExecutor(max_workers=min(8, len(ICAL_URLS) or 1)) as pool:
        futures = {pool.submit(_fetch_ical, url): url for url in ICAL_URLS}
        for future in as_completed(futures):
            for ev in future.result() or []:
                uid = ev.get("uid")
                if not uid or uid in seen:
                    continue
                seen.add(uid)
                start = ev.get("start")
                if not isinstance(start, datetime):
                    continue
                if start < _now:
                    continue
                end = ev.get("end")
                if isinstance(end, datetime):
                    if end.tzinfo is None:
                        end = end.replace(tzinfo=timezone.utc)
                else:
                    end = start
                events.append(
                    {
                        "title": ev.get("title", ""),
                        "start": start.astimezone(CPH).isoformat(),
                        "end": end.astimezone(CPH).isoformat(),
                        "location": ev.get("location", ""),
                        "subject": ev.get("subject", ""),
                    }
                )
    events.sort(key=lambda x: x["start"])
    return events


@app.post("/api/backup")
def manual_backup():
    result = backup_subjects("manual backup")
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return result


@app.get("/api/backup/history")
def backup_history():
    try:
        out = subprocess.run(
            ["git", "log", "--pretty=format:%H|%ad|%s", "--date=iso", "-n", "20"],
            cwd=str(SUBJECTS_DIR),
            check=True,
            capture_output=True,
            text=True,
        )
        lines = [line for line in out.stdout.splitlines() if line.strip()]
        history = []
        for line in lines:
            parts = line.split("|", 2)
            if len(parts) == 3:
                history.append({"commit": parts[0], "date": parts[1], "message": parts[2]})
        return history
    except Exception:
        return []


@app.post("/api/backup/dashboard")
def manual_dashboard_backup():
    result = backup_dashboard("manual frontend backup")
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return result


@app.get("/api/backup/dashboard/history")
def dashboard_backup_history():
    try:
        out = subprocess.run(
            ["git", "log", "--pretty=format:%H|%ad|%s", "--date=iso", "-n", "20"],
            cwd=str(DASHBOARD_DIR),
            check=True,
            capture_output=True,
            text=True,
        )
        lines = [line for line in out.stdout.splitlines() if line.strip()]
        history = []
        for line in lines:
            parts = line.split("|")
            if len(parts) >= 3:
                history.append({"commit": parts[0], "date": parts[1], "message": "|".join(parts[2:])})
        return history
    except Exception:
        return []


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", include_in_schema=False)
def root_index():
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return {
        "service": "mission-control-dashboard",
        "endpoints": [
            "/api/version",
            "/api/subjects",
            "/api/activity",
            "/api/profiles",
            "/api/agent/run",
            "/api/upload",
            "/api/subjects/{subject}/files",
            "/api/subjects/{subject}/notes/{filename}",
            "/api/subjects/{subject}",
            "/api/subjects/{subject}/notes/{filename}",
            "/api/subjects/{subject}/rename-note",
            "/static/",
        ],
    }
