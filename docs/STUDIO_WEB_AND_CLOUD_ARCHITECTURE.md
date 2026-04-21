# SochDB Studio Web And Cloud Architecture

## Objective
Evolve SochDB Studio from a local Tauri desktop app into a product surface that can support:

- local developer workflows
- browser-based Studio
- self-hosted deployments
- Helm/Kubernetes deployments
- eventually a managed cloud control plane

This is the right direction if we want Studio to feel more like Langfuse than a local-only admin tool.

## Short Answer
Yes, we can do this.

But the correct architecture is not:
- "run the desktop app on a server"

The correct architecture is:
- a web frontend
- a Studio backend/API service
- one or more SochDB server instances
- optional ingestion / worker services
- optional observability and evaluation services

Helm charts are a deployment mechanism for that stack.
They are not the architecture by themselves.

## Product Model

### Local Mode
User runs:
- local SochDB
- local SDK / library
- local Studio

Best for:
- development
- notebooks
- debugging
- single-user workflows

### Team / Self-Hosted Mode
User runs:
- SochDB server
- Studio web backend
- Studio web frontend
- optional Postgres / Redis / object storage depending on features

Best for:
- teams
- shared datasets
- shared search and observability
- browser access

### Cloud / Managed Mode
Provider runs:
- control plane
- managed SochDB instances
- Studio web frontend/backend
- auth / billing / usage / project management

Best for:
- SaaS / managed offering
- hosted observability and evaluation

## Recommended Architecture

### 1. SochDB Server
This is the data plane.

Responsibilities:
- indexes
- vectors
- storage
- queries
- ingestion APIs
- retrieval APIs
- stats / health

This should remain separately deployable.

### 2. Studio Backend API
This is the control-plane/backend-for-frontend layer.

Responsibilities:
- auth
- projects / workspaces
- dataset registration
- benchmark run metadata
- traces / events / search history
- query proxying to SochDB
- connection management
- aggregation for UI dashboards

This should become a standalone web service.

### 3. Studio Frontend
This is the browser UI.

Responsibilities:
- dashboards
- search playground
- dataset explorer
- index explorer
- benchmark views
- observability views
- admin views

This can reuse a lot of the current React UI from `sochdb-studio`.

### 4. Optional Worker / Job Service
Useful for:
- benchmark runs
- ingestion pipelines
- migrations
- scheduled evaluations
- long-running reindex jobs

This should be designed as optional, not required for v1.

## Key Architectural Shift
Right now Studio is heavily tied to:
- Tauri commands
- local embedded database assumptions
- direct local invocation

To support web/cloud, we should move toward:

1. Shared frontend components
2. A transport layer abstraction
3. A real backend API for Studio

Meaning:
- local desktop mode can still exist
- web mode talks to HTTP/gRPC-backed services
- both reuse the same product concepts

## Recommended Abstractions

### Frontend data layer
Introduce a client abstraction like:
- `StudioDataClient`

Backends:
- `localTauriClient`
- `remoteHttpClient`

That lets the UI stay mostly the same while changing where the data comes from.

### Product concepts
Keep the same concepts across desktop and web:
- Project
- Dataset
- Collection / Index
- Query
- Search Result
- Benchmark Run
- Trace / Event
- Connection

This avoids rebuilding the product from scratch for the web.

## Deployment Model

### Minimal self-hosted stack
- SochDB server
- Studio backend
- Studio frontend

Optional:
- Postgres for metadata
- Redis for jobs/cache
- S3-compatible object store for uploads/artifacts

### Kubernetes / Helm
Yes, Helm charts make sense here.

Likely charts:
- `sochdb-server`
- `sochdb-studio-backend`
- `sochdb-studio-frontend`
- optional `sochdb-worker`

Possible packaging:
- one umbrella chart:
  - `sochdb-platform`
- plus individual charts per component

That gives both:
- easy all-in-one install
- composable production deployment

## Recommended Helm Story

### Chart 1: `sochdb-server`
Deploys:
- SochDB server
- service
- config
- persistence

### Chart 2: `sochdb-studio`
Deploys:
- Studio backend
- Studio frontend
- ingress/service
- auth/config secrets

### Chart 3: `sochdb-platform`
Umbrella chart that includes:
- `sochdb-server`
- `sochdb-studio`
- optional workers / dependencies

This is the cleanest way to support both:
- simple local cluster installs
- larger deployments

## How Users Might Experience It

### Option A: Local developer flow
1. run `sochdb` locally
2. run Studio locally
3. inspect/search/query locally

### Option B: Self-hosted team flow
1. install via Helm
2. expose Studio via ingress
3. SDKs write/query against hosted SochDB
4. users open Studio in browser and see shared state

### Option C: Hybrid flow
1. app uses SDK locally or in service
2. events / metadata / traces are sent to hosted Studio backend
3. users inspect runs and search behavior in browser

This is where the Langfuse analogy becomes especially relevant.

## What "Like Langfuse" Really Means Here
If we mean it seriously, Studio should eventually support:

- projects and workspaces
- users and auth
- traces / histories / sessions
- benchmark and evaluation views
- search and retrieval observability
- query and result inspection
- deployment health
- configuration and policies

That means Studio becomes a real product, not just a local tool.

## What We Should Build First

### Phase 1: Web-ready Studio architecture
- keep current Tauri app working
- separate UI from local-only data access
- create a `StudioDataClient` abstraction
- define backend API contracts

### Phase 2: Web Studio MVP
- React frontend served as web app
- backend service exposing Studio APIs
- connect to SochDB server instead of local-only Tauri commands

### Phase 3: Self-hosted deployment
- Docker images
- Helm charts
- ingress/auth/config docs

### Phase 4: Managed/cloud control plane
- multi-project support
- hosted auth
- usage/billing
- org/team features

## What Not To Do

- do not try to "host the Tauri app"
- do not hardwire more UI logic directly to local Tauri commands
- do not assume Helm alone solves product architecture

## Immediate Engineering Tasks

1. Introduce a frontend data client abstraction in `sochdb-studio`.
2. Identify Tauri-specific calls that need backend API equivalents.
3. Write a draft Studio backend API surface:
   - health
   - connection
   - stats
   - tables
   - query
   - search
   - benchmark results
4. Define minimal web deployment components.
5. Only after that, start Helm chart work.

## Recommendation
Yes, build toward a web/cloud Studio.

But sequence it like this:
1. architecture separation
2. backend API
3. web frontend mode
4. Helm deployment

That is the path that gives us something production-capable instead of a fragile local-only UI wrapped in infrastructure.
