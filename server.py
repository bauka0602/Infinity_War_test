from __future__ import annotations

import json
import logging
import sys
from datetime import date
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from starlette.exceptions import HTTPException as StarletteHTTPException

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from app.auth_service import (
    confirm_teacher_claim,
    get_current_profile,
    login_user,
    logout_user,
    request_teacher_claim,
    register_user,
    require_auth_user,
    search_claimable_teachers,
    update_profile_avatar,
)
from app.admin_service import clear_all_data, clear_collection_data
from app.collections import create_collection_item, delete_collection_item, list_collection, update_collection_item
from app.config import ALLOWED_ORIGINS, DB_ENGINE, DB_FALLBACK_REASON, DB_FILE, DB_LOCK, PORT, REQUESTED_DB_ENGINE
from app.db import ensure_database, get_connection, query_all, query_one
from app.errors import ApiError
from app.import_service import generate_import_template, generate_schedule_export, import_excel_data
from app.job_store import create_schedule_generation_job, get_schedule_generation_job
from app.notification_service import (
    create_schedule_change_notifications,
    delete_all_notifications,
    delete_notification,
    list_notifications,
    mark_all_notifications_as_read,
    mark_notification_as_read,
)
from app.preference_service import (
    create_teacher_preference_request,
    delete_all_teacher_preference_requests,
    delete_teacher_preference_request,
    list_teacher_preference_requests,
    update_teacher_preference_status,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
LOGGER = logging.getLogger(__name__)


def _api_error_payload(exc: ApiError) -> dict[str, Any]:
    payload: dict[str, Any] = {"error": exc.message, "errorCode": exc.code}
    if exc.details:
        payload["details"] = exc.details
    return payload


def _http_error_payload(status_code: int, detail: Any = None) -> dict[str, Any]:
    if isinstance(detail, dict) and "error" in detail:
        return detail

    default_messages = {
        400: ("Некорректный запрос", "bad_request"),
        401: ("Требуется авторизация", "auth_required"),
        403: ("Недостаточно прав", "forbidden"),
        404: ("Not found", "not_found"),
        405: ("Method not allowed", "method_not_allowed"),
    }
    message, code = default_messages.get(status_code, ("Внутренняя ошибка сервера", "internal_server_error"))
    if isinstance(detail, str) and detail:
        message = detail
    return {"error": message, "errorCode": code}


async def _read_json(request: Request) -> dict[str, Any]:
    raw_body = await request.body()
    if not raw_body:
        return {}
    try:
        body = json.loads(raw_body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ApiError(400, "invalid_json", "Некорректный JSON") from exc
    if body is None:
        return {}
    if not isinstance(body, dict):
        raise ApiError(400, "bad_request", "Некорректный запрос")
    return body


def _query_map(request: Request) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for key, value in request.query_params.multi_items():
        result.setdefault(key, []).append(value)
    return result


def _resolve_collection_alias(collection: str) -> str:
    return {"disciplines": "courses"}.get(collection, collection)


def _require_user(request: Request) -> dict[str, Any]:
    return require_auth_user(request.headers)


def _assert_collection_access(collection: str, method: str, user: dict[str, Any]):
    if collection in {"courses", "teachers", "students", "rooms", "groups", "sections"} and user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")
    if collection == "schedules" and method in {"POST", "PUT", "DELETE"} and user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")


def _parse_int_id(raw_value: str) -> int:
    try:
        return int(raw_value)
    except ValueError as exc:
        raise ApiError(400, "invalid_id", "ID должен быть числом") from exc


def _service_info() -> dict[str, Any]:
    db_description = "PostgreSQL" if DB_ENGINE == "postgres" else f"SQLite ({DB_FILE})"
    if DB_ENGINE != REQUESTED_DB_ENGINE and DB_FALLBACK_REASON:
        db_description = f"{db_description}; fallback active"
    return {
        "message": "TimeTableG Backend API is running",
        "docs": "/docs",
        "health": "/api/health",
        "database": db_description,
    }


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_database()
    if DB_FALLBACK_REASON:
        LOGGER.warning(DB_FALLBACK_REASON)
    yield


app = FastAPI(
    title="TimeTableG Backend API",
    version="4.0.0",
    description="FastAPI version of the TimeTableG backend, fully compatible with the existing frontend.",
    lifespan=lifespan,
)

allow_all_origins = "*" in ALLOWED_ORIGINS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if allow_all_origins else ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(ApiError)
async def _handle_api_error(_request: Request, exc: ApiError):
    return JSONResponse(status_code=exc.status, content=_api_error_payload(exc))


@app.exception_handler(RequestValidationError)
async def _handle_validation_error(_request: Request, _exc: RequestValidationError):
    return JSONResponse(status_code=400, content={"error": "Некорректный запрос", "errorCode": "bad_request"})


@app.exception_handler(StarletteHTTPException)
async def _handle_http_error(request: Request, exc: StarletteHTTPException):
    if request.url.path.startswith("/api"):
        return JSONResponse(status_code=exc.status_code, content=_http_error_payload(exc.status_code, exc.detail))
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(Exception)
async def _handle_unexpected_error(request: Request, exc: Exception):
    LOGGER.exception("Unhandled API error for %s %s", request.method, request.url.path)
    if request.url.path.startswith("/api"):
        return JSONResponse(status_code=500, content={"error": "Внутренняя ошибка сервера", "errorCode": "internal_server_error"})
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


@app.get("/")
def root():
    return _service_info()


@app.get("/health")
def health_root():
    return {"status": "ok", "engine": DB_ENGINE}


@app.get("/api")
def api_root():
    return _service_info()


@app.get("/api/health")
def api_health():
    return {"status": "ok", "engine": DB_ENGINE}


@app.post("/api/auth/register", status_code=201)
async def api_register(request: Request):
    return register_user(await _read_json(request))


@app.post("/api/auth/teacher-claim/request")
async def api_teacher_claim_request(request: Request):
    return request_teacher_claim(await _read_json(request))


@app.post("/api/auth/teacher-claim/confirm")
async def api_teacher_claim_confirm(request: Request):
    return confirm_teacher_claim(await _read_json(request))


@app.post("/api/auth/login")
async def api_login(request: Request):
    return login_user(await _read_json(request))


@app.post("/api/auth/logout")
def api_logout(request: Request):
    return logout_user(request.headers)


@app.get("/api/profile")
def api_profile(request: Request):
    return get_current_profile(request.headers)


@app.post("/api/profile/avatar")
async def api_profile_avatar(request: Request):
    return update_profile_avatar(request.headers, await _read_json(request))


@app.get("/api/notifications")
def api_notifications(request: Request):
    return list_notifications(request.headers)


@app.post("/api/notifications/read-all")
def api_notifications_read_all(request: Request):
    return mark_all_notifications_as_read(request.headers)


@app.delete("/api/notifications")
def api_notifications_delete_all(request: Request):
    return delete_all_notifications(request.headers)


@app.put("/api/notifications/{notification_id}/read")
def api_notifications_read_one(notification_id: int, request: Request):
    return mark_notification_as_read(request.headers, notification_id)


@app.delete("/api/notifications/{notification_id}")
def api_notifications_delete_one(notification_id: int, request: Request):
    return delete_notification(request.headers, notification_id)


@app.get("/api/teacher-preferences/mine")
def api_teacher_preferences_mine(request: Request):
    return list_teacher_preference_requests(request.headers, mine=True)


@app.get("/api/teacher-preferences")
def api_teacher_preferences_all(request: Request):
    return list_teacher_preference_requests(request.headers, mine=False)


@app.post("/api/teacher-preferences", status_code=201)
async def api_teacher_preferences_create(request: Request):
    return create_teacher_preference_request(request.headers, await _read_json(request))


@app.delete("/api/teacher-preferences")
def api_teacher_preferences_delete_all(request: Request):
    return delete_all_teacher_preference_requests(request.headers)


@app.put("/api/teacher-preferences/{request_id}/status")
async def api_teacher_preferences_update_status(request_id: int, request: Request):
    return update_teacher_preference_status(request.headers, request_id, await _read_json(request))


@app.delete("/api/teacher-preferences/{request_id}")
def api_teacher_preferences_delete_one(request_id: int, request: Request):
    return delete_teacher_preference_request(request.headers, request_id)


@app.get("/api/public/groups")
def api_public_groups():
    with DB_LOCK:
        with get_connection() as connection:
            return query_all(
                connection,
                """
                SELECT id, name, student_count, has_subgroups, language, study_course
                FROM groups
                ORDER BY name, id
                """,
            )


@app.get("/api/public/teachers/claim-search")
def api_public_teacher_claim_search(q: str = ""):
    return search_claimable_teachers(q)


@app.post("/api/import/excel")
async def api_import_excel(request: Request):
    return import_excel_data(request.headers, await _read_json(request))


@app.get("/api/import/template")
def api_import_template(request: Request):
    template_bytes = generate_import_template(request.headers)
    return Response(
        content=template_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="timetable-import-template.xlsx"'},
    )


@app.get("/api/export/schedule")
def api_export_schedule(request: Request):
    export_bytes = generate_schedule_export(request.headers)
    return Response(
        content=export_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="schedule-export.xlsx"'},
    )


@app.post("/api/admin/clear-all")
def api_admin_clear_all(request: Request):
    return clear_all_data(request.headers)


@app.post("/api/admin/clear/{collection}")
def api_admin_clear_collection(collection: str, request: Request):
    return clear_collection_data(request.headers, collection)


@app.post("/api/schedules/generate", status_code=202)
async def api_schedule_generate(request: Request):
    user = _require_user(request)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")
    payload = await _read_json(request)
    semester = int(payload.get("semester") or 1)
    year = int(payload.get("year") or date.today().year)
    algorithm = payload.get("algorithm") or "greedy"
    return create_schedule_generation_job(semester, year, algorithm)


@app.get("/api/schedules/generate/{job_id}")
def api_schedule_generate_status(job_id: str, request: Request):
    user = _require_user(request)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")
    return get_schedule_generation_job(job_id)


@app.get("/api/{collection}")
def api_collection_list(collection: str, request: Request):
    actual_collection = _resolve_collection_alias(collection)
    if actual_collection not in {"courses", "teachers", "students", "rooms", "groups", "schedules", "sections"}:
        raise ApiError(404, "not_found", "Not found")
    user = _require_user(request)
    _assert_collection_access(actual_collection, "GET", user)
    with DB_LOCK:
        with get_connection() as connection:
            return list_collection(connection, actual_collection, _query_map(request), user)


@app.post("/api/{collection}", status_code=201)
async def api_collection_create(collection: str, request: Request):
    actual_collection = _resolve_collection_alias(collection)
    if actual_collection not in {"courses", "teachers", "students", "rooms", "groups", "schedules", "sections"}:
        raise ApiError(404, "not_found", "Not found")
    user = _require_user(request)
    _assert_collection_access(actual_collection, "POST", user)
    payload = await _read_json(request)
    with DB_LOCK:
        with get_connection() as connection:
            created = create_collection_item(connection, actual_collection, payload)
            if actual_collection == "schedules":
                create_schedule_change_notifications(connection, after_item=created)
            return created


@app.get("/api/{collection}/{item_id}")
def api_collection_get_one(collection: str, item_id: str, request: Request):
    actual_collection = _resolve_collection_alias(collection)
    if actual_collection not in {"courses", "teachers", "students", "rooms", "groups", "schedules", "sections"}:
        raise ApiError(404, "not_found", "Not found")
    item_id_int = _parse_int_id(item_id)
    user = _require_user(request)
    _assert_collection_access(actual_collection, "GET", user)
    with DB_LOCK:
        with get_connection() as connection:
            items = list_collection(connection, actual_collection, _query_map(request), user)
            item = next((row for row in items if int(row.get("id")) == item_id_int), None)
            if item is None:
                raise ApiError(404, "record_not_found", "Запись не найдена")
            return item


@app.put("/api/{collection}/{item_id}")
async def api_collection_update(collection: str, item_id: str, request: Request):
    actual_collection = _resolve_collection_alias(collection)
    if actual_collection not in {"courses", "teachers", "students", "rooms", "groups", "schedules", "sections"}:
        raise ApiError(404, "not_found", "Not found")
    item_id_int = _parse_int_id(item_id)
    user = _require_user(request)
    _assert_collection_access(actual_collection, "PUT", user)
    payload = await _read_json(request)
    with DB_LOCK:
        with get_connection() as connection:
            existing = query_one(connection, f"SELECT * FROM {actual_collection} WHERE id = ?", (item_id_int,))
            if existing is None:
                raise ApiError(404, "record_not_found", "Запись не найдена")
            updated = update_collection_item(connection, actual_collection, item_id_int, payload)
            if actual_collection == "schedules":
                create_schedule_change_notifications(connection, before_item=existing, after_item=updated)
            return updated


@app.delete("/api/{collection}/{item_id}")
def api_collection_delete(collection: str, item_id: str, request: Request):
    actual_collection = _resolve_collection_alias(collection)
    if actual_collection not in {"courses", "teachers", "students", "rooms", "groups", "schedules", "sections"}:
        raise ApiError(404, "not_found", "Not found")
    item_id_int = _parse_int_id(item_id)
    user = _require_user(request)
    _assert_collection_access(actual_collection, "DELETE", user)
    if actual_collection == "sections":
        raise ApiError(405, "method_not_allowed", "Удаление секций недоступно")
    with DB_LOCK:
        with get_connection() as connection:
            existing = query_one(connection, f"SELECT * FROM {actual_collection} WHERE id = ?", (item_id_int,))
            if existing is None:
                raise ApiError(404, "record_not_found", "Запись не найдена")
            delete_collection_item(connection, actual_collection, item_id_int)
            if actual_collection == "schedules":
                create_schedule_change_notifications(connection, before_item=existing)
            return {"success": True}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
