# SochDB Studio Workbench V1

## Why this exists

SochDB Studio should become the product-facing surface for:

- local embedded SochDB usage
- 2.0 client / server usage
- search and retrieval workflows
- vector and index visibility
- observability and traces
- benchmark and evidence views

Right now the repo already has a solid Tauri + React base, but the frontend is still mostly concentrated in `src/App.tsx`.

This note defines the first ML-facing Studio slice without disrupting the current app all at once.

## Product goal

Studio should feel like a mix of:

- database administration console
- retrieval/search playground
- observability dashboard
- benchmark/evidence viewer

It should help someone answer:

1. What data is stored?
2. What vectors and indexes exist?
3. What search returns?
4. What mode am I using: local or 2.0?
5. What metrics have we measured versus only claimed?

## V1 scope

### 1. Overview

Cards for:

- mode
- dataset
- vector count
- embedding dimension
- recall
- latency

### 2. Search Playground

- sample query selector
- search results
- score / distance
- local vs 2.0 comparison framing

### 3. Dataset Explorer

- sample SciFact documents
- sample SciFact queries
- dataset summary

### 4. Index Explorer

- index name
- HNSW settings
- vector count
- build time
- search path

### 5. Observability

Mirror or embed the existing SochDB observability story:

- request rate
- request latency
- active connections
- memory usage
- vector operations / second
- server status

### 6. Evidence Panel

Keep these separate:

- measured by us
- observed from live server
- claimed in PRs / releases

## Recommended frontend structure

Instead of continuing to grow one giant `App.tsx`, move toward:

```text
src/
  app/
    shell/
    routes/
  features/
    overview/
    search/
    datasets/
    indexes/
    observability/
    evidence/
  components/
    cards/
    layout/
    charts/
  data/
    fixtures/
  lib/
    formatting/
    adapters/
```

This does not need to happen in one PR. The first goal is just to create a clear place for ML-facing Studio work.

## Suggested first implementation steps

1. Add a new `Workbench` route or tab in Studio
2. Start with fixture-backed cards and panels
3. Reuse real benchmark numbers from:
   - local SciFact tuned run
   - remote 2.0 SciFact run
4. Add a clean local vs 2.0 mode switch
5. Add evidence labeling from day one

## Data sources for V1

Use static or imported artifacts first:

- `sochdb/benchmarks/retrieval/datasets/scifact/*`
- `sochdb/benchmarks/retrieval/results/sochdb_scifact_m48_ef200.json`
- `sochdb-2.0/benchmarks/retrieval/results/sochdb_grpc_scifact_remote.json`
- `sochdb/docker/grafana/provisioning/dashboards/sochdb-overview.json`

## Out of scope for V1

- auth
- live uploads
- persistent Studio-side projects
- full backend synchronization

V1 should be showable and product-shaped first.
