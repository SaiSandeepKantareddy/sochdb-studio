# SochDB Studio Self-Hosted VM Deployment

This is the fastest path to a shared Studio browser experience on a single VM.

## What it deploys

- `studio-backend`
  - Studio API
  - project/workspace state
  - API keys
  - ingestion endpoint
  - remote gRPC connection manager
- `studio-frontend`
  - browser UI served by nginx
  - proxies `/api/*` to the backend

## Requirements

- Docker
- Docker Compose plugin
- A reachable SochDB gRPC server for remote instances

## Start

```bash
cd deploy/self-hosted
docker compose -f docker-compose.vm.yml up -d --build
```

## Open

- Frontend: `http://<server-ip>`
- Backend health: `http://127.0.0.1:4318/health` (server-local only)

## Notes

- This does not deploy the SochDB gRPC server itself.
- In Studio, create a remote instance pointing at your SochDB server host/port.
- The backend is intentionally bound to `127.0.0.1` while the frontend proxies `/api/*` to it.
- That means browser clients can still use Studio normally, but the raw backend port is no longer public by default.
- If the SochDB gRPC server runs on the same VM outside Docker, use `host.docker.internal` as the Studio remote host.
- For a more Langfuse-like feel, issue a project API key in Studio and send events to:

```bash
POST /api/studio/ingest/events
Authorization: Bearer soch_sk_...
```
