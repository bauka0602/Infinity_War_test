# Merge notes

This version merges the legacy API from `timetableg-backend-main` into `Diploma_Version2.0` while preserving the improved FastAPI optimizer endpoints.

## Added legacy API endpoints
- `POST /api/auth/register`
- `POST /api/auth/login`
- `GET /api/profile`
- `POST /api/profile/avatar`
- `POST /api/import/excel`
- `GET /api/import/template`
- `POST /api/admin/clear-all`
- `POST /api/admin/clear/{collection}`
- `GET/POST /api/{collection}` for `courses`, `teachers`, `rooms`, `schedules`, `sections`
- `GET/PUT/DELETE /api/{collection}/{item_id}`
- `POST /api/schedules/generate` (legacy simple generator)

## Preserved optimizer endpoints
- `POST /api/optimizer/run`
- `POST /api/optimizer/generate`
- `POST /api/optimizer/export-excel`
- `GET /api/optimizer/template-excel`
- `POST /api/optimizer/import-excel`
- `POST /api/optimizer/import-excel-export`

## Internal changes
- Restored legacy database, auth, admin and Excel import modules.
- Fixed the duplicated `CREATE TABLE` typo in legacy `app/db.py`.
- Added database initialization on FastAPI startup.
- Kept the improved optimizer from `Diploma_Version2.0` as the main optimization layer.
