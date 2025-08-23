#!/usr/bin/env python3
import os, json, datetime, pathlib
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text

app = Flask(__name__)

# --- Config ---
PORT = int(os.getenv("PORT", "5000"))
DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN")  # optional shared secret; expect "Authorization: Bearer <token>"

engine = create_engine(DATABASE_URL) if DATABASE_URL else None
_ensured = False

def ensure_table():
    global _ensured
    if _ensured or engine is None:  # allow running without DB
        return
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS public.skiptrace_webhook_events (
          event_id    bigserial PRIMARY KEY,
          received_at timestamptz NOT NULL DEFAULT now(),
          job_id      text,
          status      text,
          event_type  text,
          payload     jsonb,
          headers     jsonb
        );
        """))
    _ensured = True

def insert_event(job_id, status, event_type, payload, headers):
    if engine is None:
        return
    ensure_table()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO public.skiptrace_webhook_events
                  (job_id, status, event_type, payload, headers)
                VALUES
                  (:job_id, :status, :event_type, CAST(:payload AS jsonb), CAST(:headers AS jsonb))
            """),
            {
                "job_id": job_id,
                "status": status,
                "event_type": event_type,
                "payload": json.dumps(payload if payload is not None else {}),
                "headers": json.dumps(headers if headers is not None else {}),
            }
        )

def extract_job_and_status(payload: dict | None):
    """Best-effort extraction from unknown vendor schema."""
    job_id = None
    status = None
    event_type = None
    p = payload or {}

    # job id candidates
    for k in ("jobId", "job_id", "id"):
        if isinstance(p.get(k), (str, int)):
            job_id = str(p[k]); break
    if not job_id and isinstance(p.get("data"), dict):
        for k in ("jobId", "job_id", "id"):
            v = p["data"].get(k)
            if isinstance(v, (str, int)):
                job_id = str(v); break

    # status candidates
    if isinstance(p.get("status"), dict):
        status = p["status"].get("text") or p["status"].get("state") or p["status"].get("message")
    elif isinstance(p.get("status"), (str, int)):
        status = str(p["status"])
    elif isinstance(p.get("data"), dict) and isinstance(p["data"].get("status"), (str, int)):
        status = str(p["data"]["status"])

    # event type / action
    for k in ("event", "type", "action"):
        if isinstance(p.get(k), str):
            event_type = p[k]; break

    return job_id, status, event_type

def save_backup_file(payload: dict | None):
    ts = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S-%f")
    outdir = pathlib.Path("webhook_events")
    outdir.mkdir(parents=True, exist_ok=True)
    fp = outdir / f"batchdata_{ts}.json"
    with fp.open("w", encoding="utf-8") as f:
        json.dump(payload if payload is not None else {}, f, indent=2)
    return str(fp)

@app.route("/healthz")
def healthz():
    return {"ok": True, "time": datetime.datetime.utcnow().isoformat() + "Z"}, 200

@app.route("/webhooks/batchdata/skiptrace", methods=["POST"])
def batchdata_webhook():
    # Optional simple auth
    if WEBHOOK_TOKEN:
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer ") or auth.split(" ", 1)[1] != WEBHOOK_TOKEN:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True)
    # Fallback: accept form-data with a JSON string part named "payload"
    if payload is None and "payload" in request.form:
        try:
            payload = json.loads(request.form["payload"])
        except Exception:
            payload = {"raw": request.form.get("payload")}

    # Persist to disk as a safety net
    backup_path = save_backup_file(payload)

    # Persist to DB (if DATABASE_URL provided)
    headers_dict = dict(request.headers.items())
    job_id, status, event_type = extract_job_and_status(payload)
    try:
        insert_event(job_id, status, event_type, payload, headers_dict)
    except Exception as e:
        # Don't fail the webhook; just log
        app.logger.exception("DB insert failed: %s", e)

    return jsonify({
        "ok": True,
        "saved": {"backup_path": backup_path},
        "parsed": {"job_id": job_id, "status": status, "event_type": event_type}
    }), 200

if __name__ == "__main__":
    print(f"→ Webhook server listening on http://0.0.0.0:{PORT}")
    if not DATABASE_URL:
        print("⚠️  DATABASE_URL not set — events will only be saved to webhook_events/*.json")
    app.run(host="0.0.0.0", port=PORT)