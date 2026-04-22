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

- Frontend: `http://<server-ip>:3000`
- Backend health: `http://<server-ip>:4318/health`

## Notes

- This does not deploy the SochDB gRPC server itself.
- In Studio, create a remote instance pointing at your SochDB server host/port.
- For a more Langfuse-like feel, issue a project API key in Studio and send events to:

```bash
POST /api/studio/ingest/events
Authorization: Bearer soch_sk_...
```
