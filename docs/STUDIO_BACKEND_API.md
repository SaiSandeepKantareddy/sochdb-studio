# Studio Backend API

This is the first HTTP backend surface for web/cloud Studio.

## Endpoints

### `GET /health`
Basic service health.

### `GET /api/studio/health`
Studio backend status and current connection summary.

### `POST /api/studio/connect`
Request body:

```json
{
  "path": "/path/to/data"
}
```

Starts a backend MCP session against the given SochDB data path.

### `POST /api/studio/disconnect`
Stops the active backend MCP session.

### `GET /api/studio/stats`
Returns Studio-facing database stats.

### `GET /api/studio/tables`
Returns:

```json
{
  "tables": ["users", "products"]
}
```

### `POST /api/studio/query`
Request body:

```json
{
  "query": "SELECT * FROM users LIMIT 3"
}
```

Returns Studio-style tabular query results.

### `POST /api/studio/mcp/call`
Pass-through MCP tool call:

```json
{
  "toolName": "sochdb_get",
  "arguments": {
    "path": "users/1"
  }
}
```

### `GET /api/studio/debug/session`
Development diagnostics for the active MCP session.

## Current implementation notes

- this backend currently uses `sochdb-mcp` over stdio
- it keeps one active session in memory
- it is a stepping stone toward a real multi-project web backend
- stats are best-effort right now and should eventually come from a dedicated server/control-plane API

## Environment

### Backend
- `STUDIO_BACKEND_PORT`
- `SOCHDB_MCP_BIN` (optional path to built `sochdb-mcp`)

If `SOCHDB_MCP_BIN` is not provided, the backend falls back to:
- `cargo run --manifest-path ../sochdb/sochdb-mcp/Cargo.toml -- --db <path>`

### Frontend
- `VITE_STUDIO_API_BASE_URL`

Example:

```bash
VITE_STUDIO_API_BASE_URL=http://127.0.0.1:4318 npm run dev
```
