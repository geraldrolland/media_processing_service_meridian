"""Readiness check route."""

from fastapi import APIRouter
from sqlalchemy import text

from app.db_config import async_session_factory
from app.lock import redis_client

router = APIRouter()


@router.get("/ready")
async def ready():
    checks = {}
    try:
        redis_client.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"failed: {e}"

    try:
        async with async_session_factory() as session:
            await session.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as e:
        checks["database"] = f"failed: {e}"

    all_ok = all(v == "ok" for v in checks.values())
    return {"status": "ready" if all_ok else "not_ready", "checks": checks}
