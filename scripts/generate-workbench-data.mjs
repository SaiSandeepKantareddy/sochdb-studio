import fs from "node:fs";
import path from "node:path";

const studioRoot = process.cwd();
const workspaceRoot = path.resolve(studioRoot, "..");

const localResultPath = path.join(
  workspaceRoot,
  "sochdb",
  "benchmarks",
  "retrieval",
  "results",
  "sochdb_scifact_m48_ef200.json"
);
const remoteResultPath = path.join(
  workspaceRoot,
  "sochdb-2.0",
  "benchmarks",
  "retrieval",
  "results",
  "sochdb_grpc_scifact_remote.json"
);
const corpusPath = path.join(
  workspaceRoot,
  "sochdb",
  "benchmarks",
  "retrieval",
  "datasets",
  "scifact",
  "corpus.jsonl"
);
const metadataPath = path.join(
  workspaceRoot,
  "sochdb",
  "benchmarks",
  "retrieval",
  "datasets",
  "scifact",
  "metadata.json"
);
const outputPath = path.join(
  studioRoot,
  "src",
  "features",
  "workbench",
  "generated",
  "workbenchSnapshot.json"
);

const selectedQueryIds = ["1", "36", "42"];
const selectedDocIds = ["4983", "5836", "33370"];

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function formatNumber(value) {
  return new Intl.NumberFormat("en-US").format(value);
}

function formatMs(value, digits = 3) {
  return `${Number(value).toFixed(digits)} ms`;
}

function buildDocs(docIds) {
  const docSet = new Set(docIds);
  const docs = [];
  const lines = fs.readFileSync(corpusPath, "utf8").trim().split("\n");

  for (const line of lines) {
    const row = JSON.parse(line);
    if (!docSet.has(row.id)) continue;
    docs.push({
      id: row.id,
      title: row.title || `Document ${row.id}`,
      body: row.body
    });
    if (docs.length === docIds.length) break;
  }

  return docIds
    .map((id) => docs.find((doc) => doc.id === id))
    .filter(Boolean);
}

function buildQueries(resultJson, noteMode) {
  return selectedQueryIds.map((queryId) => {
    const entry = resultJson.queries.find((query) => query.query_id === queryId);
    if (!entry) {
      throw new Error(`Missing query ${queryId} in ${resultJson.system}`);
    }

    return {
      id: entry.query_id,
      text: entry.query,
      latency: formatMs(entry.latency_ms),
      results: entry.results.slice(0, 3).map((result, index) => ({
        id: result.doc_id,
        distance: Number(result.distance).toFixed(4),
        note:
          noteMode === "local"
            ? index === 0
              ? "Top local match from the tuned embedded SciFact run."
              : "Returned from the tuned embedded HNSW index."
            : index === 0
              ? "Top remote match returned by the live gRPC server."
              : "Returned remotely from the 2.0 client/server search path."
      }))
    };
  });
}

function buildMode(mode, resultJson, metadata, docs) {
  const isLocal = mode === "local";
  const build = resultJson.build ?? {};
  const queryLatency = resultJson.query_latency ?? {};
  const evidence = isLocal
    ? [
        {
          title: "Measured by us: local tuned SciFact",
          body:
            "recall@5 0.7109 · MRR 0.5883 · nDCG@5 0.6135 · p50 0.304 ms"
        },
        {
          title: "Local quality takeaway",
          body:
            "The tuned embedded preset matched SQLite + FAISS on SciFact quality while staying extremely fast on a local workflow."
        },
        {
          title: "Claimed in 2.0 PR",
          body:
            "Insert throughput of 81,776 vec/s vs Qdrant 7,492 vec/s is the strongest visible 2.0 claim, but methodology still needs fuller documentation."
        }
      ]
    : [
        {
          title: "Measured by us: remote 2.0 SciFact",
          body:
            "recall@5 0.7109 · MRR 0.5883 · nDCG@5 0.6135 · p50 143.889 ms"
        },
        {
          title: "Observed from live server",
          body:
            "The existing bench_final index on the live server showed 10,000 vectors at 128 dimensions, which does not match every public benchmark mention."
        },
        {
          title: "Claimed in 2.0 PR",
          body:
            "Search quality matched the local tuned run in our SciFact check, while the broader 2.0 story emphasizes higher insert throughput and vector-engine performance."
        }
      ];

  const observability = isLocal
    ? [
        ["Request Rate", "local"],
        ["Request Latency", formatMs(queryLatency.p50_ms)],
        ["Active Connections", "1 client"],
        ["Memory Usage", "embedded"],
        ["Vector Operations / sec", "tracked"],
        ["Server Status", "local"]
      ]
    : [
        ["Request Rate", "tracked"],
        ["Request Latency", formatMs(queryLatency.p50_ms)],
        ["Active Connections", "remote"],
        ["Memory Usage", "tracked"],
        ["Vector Operations / sec", "tracked"],
        ["Server Status", "SERVING"]
      ];

  return {
    label: isLocal ? "Local Embedded" : "2.0 Client / Server",
    modeBadge: isLocal ? "Local embedded mode" : "2.0 client / server mode",
    dataset: "SciFact",
    vectors: formatNumber(resultJson.corpus_size),
    queryCount: formatNumber(resultJson.query_count),
    dimension: String(resultJson.embedding_dimension),
    recall: "0.7109",
    recallSub: isLocal ? "Tuned local SciFact run" : "Remote gRPC run matched local quality",
    latencyP50: formatMs(queryLatency.p50_ms),
    indexSummary: isLocal
      ? `m=${build.m} · ef=${build.ef_construction} · ${build.precision ?? "f32"}`
      : `m=${build.m} · ef=${build.ef_construction} · ef_search=${build.ef_search}`,
    modePill: isLocal ? "Local embedded path" : "2.0 remote path",
    qualityPill: isLocal ? "Baseline quality run" : "Quality matched local tuned run",
    latencyNote: isLocal ? "Latency is local-only" : "Latency includes network + service overhead",
    indexName: isLocal ? "sochdb_scifact_m48_ef200" : resultJson.storage.index_name,
    buildTime: isLocal ? formatMs(build.index_build_time_ms) : formatMs(build.insert_time_ms),
    searchPath: isLocal ? "Local search" : "Remote gRPC",
    indexNote: isLocal
      ? `This run used HNSW settings m=${build.m} and ef_construction=${build.ef_construction} with ${resultJson.embedding_model} (${resultJson.embedding_dimension} dims).`
      : `The remote SciFact run wrote to ${resultJson.storage.index_name} on ${resultJson.storage.endpoint} and preserved the same retrieval quality as the local tuned run.`,
    datasetFacts: [
      `${formatNumber(metadata.corpus_count)} documents · ${formatNumber(metadata.query_count)} labeled queries · ${formatNumber(metadata.qrels_count)} qrels.`,
      `Embedding model: ${resultJson.embedding_model}.`,
      isLocal
        ? "This view reflects the embedded Python-first retrieval path."
        : "This view reflects the remote 2.0 client/server retrieval path."
    ],
    docs,
    queries: buildQueries(resultJson, isLocal ? "local" : "remote"),
    evidence,
    observability: observability.map(([label, value]) => ({ label, value })),
    traceSteps: [
      {
        title: "1. Ingest",
        body: "Show uploaded documents, chunk counts, metadata, and ingestion errors instead of hiding everything behind scripts."
      },
      {
        title: "2. Embed + Index",
        body: "Surface embedding model, dimension, distance metric, HNSW settings, and build time for the active index."
      },
      {
        title: "3. Search + Observe",
        body: "Connect search results to latency, server status, and trace-like events so users can understand what SochDB is actually doing."
      }
    ]
  };
}

const localResult = readJson(localResultPath);
const remoteResult = readJson(remoteResultPath);
const metadata = readJson(metadataPath);
const docs = buildDocs(selectedDocIds);

const snapshot = {
  generatedAt: new Date().toISOString(),
  sources: {
    localResultPath: path.relative(studioRoot, localResultPath),
    remoteResultPath: path.relative(studioRoot, remoteResultPath),
    corpusPath: path.relative(studioRoot, corpusPath),
    metadataPath: path.relative(studioRoot, metadataPath)
  },
  workbenchData: {
    local: buildMode("local", localResult, metadata, docs),
    remote: buildMode("remote", remoteResult, metadata, docs)
  }
};

fs.mkdirSync(path.dirname(outputPath), { recursive: true });
fs.writeFileSync(outputPath, JSON.stringify(snapshot, null, 2) + "\n");
console.log(`Wrote ${path.relative(studioRoot, outputPath)}`);
