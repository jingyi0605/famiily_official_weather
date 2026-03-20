from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from app.db.engine import build_database_engine
from app.modules.device_integration.schemas import IntegrationSyncPluginPayload

from .service import WEATHER_PLUGIN_ID, run_weather_integration_sync


def sync(payload: dict | None = None) -> dict:
    raw_payload = payload or {}
    try:
        request = IntegrationSyncPluginPayload.model_validate(raw_payload)
    except Exception as exc:
        return _error_result(
            plugin_id=str(raw_payload.get("plugin_id") or WEATHER_PLUGIN_ID),
            sync_scope=str(raw_payload.get("sync_scope") or "device_sync"),
            error_code="integration_payload_invalid",
            error_message=f"天气插件执行参数不合法: {exc}",
        )

    db = _extract_db_session(raw_payload)
    if db is not None:
        try:
            return run_weather_integration_sync(
                db,
                household_id=request.household_id,
                integration_instance_id=request.integration_instance_id,
                sync_scope=request.sync_scope,
                selected_external_ids=request.selected_external_ids,
                options=request.options,
            )
        except Exception as exc:
            return _error_result(
                plugin_id=request.plugin_id,
                sync_scope=request.sync_scope,
                error_code="integration_runtime_failed",
                error_message=str(exc),
            )

    database_url = _extract_database_url(raw_payload)
    if not database_url:
        return _error_result(
            plugin_id=request.plugin_id,
            sync_scope=request.sync_scope,
            error_code="integration_runtime_missing",
            error_message="缺少插件运行时数据库上下文。",
        )

    engine = build_database_engine(database_url)
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)
    try:
        with session_factory() as db:
            return run_weather_integration_sync(
                db,
                household_id=request.household_id,
                integration_instance_id=request.integration_instance_id,
                sync_scope=request.sync_scope,
                selected_external_ids=request.selected_external_ids,
                options=request.options,
            )
    except Exception as exc:
        return _error_result(
            plugin_id=request.plugin_id,
            sync_scope=request.sync_scope,
            error_code="integration_runtime_failed",
            error_message=str(exc),
        )
    finally:
        engine.dispose()


def _extract_database_url(payload: dict[str, Any]) -> str | None:
    system_context = payload.get("_system_context")
    if not isinstance(system_context, dict):
        return None
    for context_key in ("integration_runtime", "device_integration"):
        context_payload = system_context.get(context_key)
        if not isinstance(context_payload, dict):
            continue
        database_url = context_payload.get("database_url")
        if isinstance(database_url, str) and database_url.strip():
            return database_url.strip()
    return None


def _extract_db_session(payload: dict[str, Any]) -> Session | None:
    system_context = payload.get("_system_context")
    if not isinstance(system_context, dict):
        return None
    for context_key in ("integration_runtime", "device_integration"):
        context_payload = system_context.get(context_key)
        if not isinstance(context_payload, dict):
            continue
        db_session = context_payload.get("db_session")
        if isinstance(db_session, Session):
            return db_session
    return None


def _error_result(
    *,
    plugin_id: str,
    sync_scope: str,
    error_code: str,
    error_message: str,
) -> dict:
    return {
        "sync_scope": sync_scope,
        "message": "天气同步失败",
        "items": [],
        "summary": {
            "created_devices": 0,
            "updated_devices": 0,
            "created_bindings": 0,
            "devices": [],
            "failures": [{"reason": error_message}],
        },
        "dashboard_snapshots": [],
        "records": [],
        "instance_status": {
            "success": False,
            "degraded": True,
            "error_code": error_code,
            "error_message": error_message,
        },
        "plugin_id": plugin_id,
    }
