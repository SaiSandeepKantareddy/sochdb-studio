import snapshot from "./generated/workbenchSnapshot.json";

export type WorkbenchMode = "local" | "remote";

export type WorkbenchDoc = {
  id: string;
  title: string;
  body: string;
};

export type WorkbenchResult = {
  id: string;
  distance: string;
  note: string;
};

export type WorkbenchQuery = {
  id: string;
  text: string;
  latency: string;
  results: WorkbenchResult[];
};

export type WorkbenchEvidence = {
  title: string;
  body: string;
};

export type WorkbenchStat = {
  label: string;
  value: string;
};

export type WorkbenchTraceStep = {
  title: string;
  body: string;
};

export type WorkbenchModeData = {
  label: string;
  modeBadge: string;
  dataset: string;
  vectors: string;
  queryCount: string;
  dimension: string;
  recall: string;
  recallSub: string;
  latencyP50: string;
  indexSummary: string;
  modePill: string;
  qualityPill: string;
  latencyNote: string;
  indexName: string;
  buildTime: string;
  searchPath: string;
  indexNote: string;
  datasetFacts: string[];
  docs: WorkbenchDoc[];
  queries: WorkbenchQuery[];
  evidence: WorkbenchEvidence[];
  observability: WorkbenchStat[];
  traceSteps: WorkbenchTraceStep[];
};

export const workbenchData = snapshot.workbenchData as Record<WorkbenchMode, WorkbenchModeData>;
export const workbenchSources = snapshot.sources;
export const workbenchGeneratedAt = snapshot.generatedAt;
