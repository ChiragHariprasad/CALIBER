"""HTTP entry point for the CALIBRE operational and UI integration API.

Run inside WSL with: ``uvicorn api:app --host 127.0.0.1 --port 8000``.
"""
from __future__ import annotations

import os
from pathlib import Path

from fastapi.staticfiles import StaticFiles

from dashboard.service import DashboardService, create_dashboard_app

BOOTSTRAP_SERVERS = os.environ.get("CALIBRE_KAFKA_BOOTSTRAP", "127.0.0.1:9092")
PERSISTENCE_DB = Path(os.environ.get(
    "CALIBRE_PERSISTENCE_DB",
    Path(__file__).resolve().parent / "data" / "calibre_persistence.db",
))

dashboard_service = DashboardService(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    run_id=os.environ.get("CALIBRE_API_CONSUMER_ID", "api"),
)
app = create_dashboard_app(dashboard_service, persistence_db_path=PERSISTENCE_DB)
app.mount("/ui", StaticFiles(directory=Path(__file__).resolve().parent / "ui", html=True), name="ui")


@app.on_event("startup")
def start_dashboard_consumer() -> None:
    dashboard_service.start()


@app.on_event("shutdown")
def stop_dashboard_consumer() -> None:
    dashboard_service.stop()
