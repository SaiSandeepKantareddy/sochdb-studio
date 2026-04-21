import { useEffect, useMemo, useState } from "react";
import { studioDataClient } from "../../lib/studioDataClient";
import { workbenchData, workbenchGeneratedAt, type WorkbenchMode } from "./workbenchData";

type LiveStats = {
  memtable_size_bytes: number;
  wal_size_bytes: number;
  total_tables: number;
  total_rows: number;
  namespace_count?: number;
  health_status?: string;
  active_transactions: number;
  version: string;
};

type QueryResult = {
  columns: string[];
  rows: unknown[][];
  stats: {
    row_count: number;
    execution_time_ms: number;
    scanned_rows: number;
  };
};

type ConnectionInfo = {
  instanceType?: string | null;
  endpoint?: string | null;
  features?: {
    query: boolean;
    mcp: boolean;
    write: boolean;
  };
};

function MetricCard({
  label,
  value,
  sub,
}: {
  label: string;
  value: string;
  sub: string;
}) {
  return (
    <div className="glass-panel rounded-2xl p-4">
      <div className="text-xs uppercase tracking-wider text-text-muted mb-2">{label}</div>
      <div className="text-2xl font-bold text-text-default tracking-tight">{value}</div>
      <div className="text-xs text-text-muted mt-2 leading-relaxed">{sub}</div>
    </div>
  );
}

function SectionHeader({
  title,
  subtitle,
  badge,
}: {
  title: string;
  subtitle: string;
  badge?: string;
}) {
  return (
    <div className="flex items-start justify-between gap-3 mb-5">
      <div>
        <h2 className="text-xl font-bold text-text-default mb-1">{title}</h2>
        <p className="text-sm text-text-muted">{subtitle}</p>
      </div>
      {badge && (
        <div className="px-3 py-1.5 rounded-full border border-border-default bg-background-muted text-xs text-text-muted whitespace-nowrap">
          {badge}
        </div>
      )}
    </div>
  );
}

export default function WorkbenchView({
  connected = false,
  appTables = [],
  appStatus = null,
  appConnection = null,
}: {
  connected?: boolean;
  appTables?: string[];
  appStatus?: LiveStats | null;
  appConnection?: ConnectionInfo | null;
}) {
  const [mode, setMode] = useState<WorkbenchMode>("local");
  const [selectedQueryId, setSelectedQueryId] = useState("1");
  const [selectedDocId, setSelectedDocId] = useState("4983");
  const [liveStats, setLiveStats] = useState<LiveStats | null>(null);
  const [liveTables, setLiveTables] = useState<string[]>([]);
  const [liveError, setLiveError] = useState<string | null>(null);
  const [liveLoading, setLiveLoading] = useState(false);
  const [liveQuery, setLiveQuery] = useState("SELECT * FROM users LIMIT 3");
  const [liveQueryResult, setLiveQueryResult] = useState<QueryResult | null>(null);
  const [liveQueryError, setLiveQueryError] = useState<string | null>(null);
  const [liveQueryLoading, setLiveQueryLoading] = useState(false);
  const [lastRefreshAt, setLastRefreshAt] = useState<Date | null>(null);
  const [lastExecutedQuery, setLastExecutedQuery] = useState<string | null>(null);
  const [remoteCatalogTable, setRemoteCatalogTable] = useState<string | null>(null);
  const [remoteCatalogDetail, setRemoteCatalogDetail] = useState<Record<string, unknown> | null>(null);
  const [remoteCatalogLoading, setRemoteCatalogLoading] = useState(false);
  const [remoteCatalogError, setRemoteCatalogError] = useState<string | null>(null);
  const [remoteCatalogRows, setRemoteCatalogRows] = useState<Array<Record<string, unknown>>>([]);
  const [remoteNamespaces, setRemoteNamespaces] = useState<Array<Record<string, unknown>>>([]);
  const [remoteCatalogFilter, setRemoteCatalogFilter] = useState("");

  const isRemoteConnection = appConnection?.instanceType === "remote";
  const liveQueryExamples = isRemoteConnection
    ? ["SHOW COLLECTIONS", "SELECT * FROM collections", "SHOW NAMESPACES"]
    : ["SELECT * FROM users LIMIT 3", "SELECT * FROM products LIMIT 3", "users", "users/1"];

  const current = workbenchData[mode];
  const selectedQuery = useMemo(
    () => current.queries.find((query) => query.id === selectedQueryId) ?? current.queries[0],
    [current, selectedQueryId]
  );
  const selectedDoc = useMemo(
    () => current.docs.find((doc) => doc.id === selectedDocId) ?? current.docs[0],
    [current, selectedDocId]
  );

  const updateLiveQuery = (nextQuery: string) => {
    setLiveQuery(nextQuery);
    setLiveQueryError(null);
  };

  const refreshLiveState = async () => {
    if (!connected) return;

    setLiveLoading(true);
    setLiveError(null);

    if (appTables.length > 0) {
      setLiveTables(appTables);
    }
    if (appStatus) {
      setLiveStats(appStatus);
    }

    try {
      const [stats, tableNames] = await Promise.all([
        studioDataClient.getStats() as Promise<LiveStats>,
        studioDataClient.listTables(),
      ]);

      setLiveStats(stats);

      setLiveTables(tableNames);
      if (tableNames.length > 0 && !liveQuery.trim()) {
        updateLiveQuery(`SELECT * FROM ${tableNames[0]} LIMIT 3`);
      }
      setLastRefreshAt(new Date());
    } catch (error: unknown) {
      setLiveError(error instanceof Error ? error.message : String(error));
    } finally {
      setLiveLoading(false);
    }
  };

  const loadRemoteCatalogDetail = async (tableName: string) => {
    if (!connected || !isRemoteConnection) return;

    setRemoteCatalogTable(tableName);
    setRemoteCatalogLoading(true);
    setRemoteCatalogError(null);

    try {
      const response = await studioDataClient.mcpCallTool("sochdb_describe", { table: tableName });
      const text = response?.content?.[0]?.text ?? "";
      setRemoteCatalogDetail(text ? JSON.parse(text) : null);
    } catch (error: unknown) {
      setRemoteCatalogDetail(null);
      setRemoteCatalogError(error instanceof Error ? error.message : String(error));
    } finally {
      setRemoteCatalogLoading(false);
    }
  };

  const loadRemoteCatalog = async () => {
    if (!connected || !isRemoteConnection) return;

    try {
      const [collectionsResponse, namespacesResponse] = await Promise.all([
        studioDataClient.mcpCallTool("sochdb_query", { query: "SELECT * FROM collections" }),
        studioDataClient.mcpCallTool("sochdb_query", { query: "SHOW NAMESPACES" }),
      ]);

      const collectionText = collectionsResponse?.content?.[0]?.text ?? "[]";
      const namespaceText = namespacesResponse?.content?.[0]?.text ?? "[]";

      const collectionsParsed = JSON.parse(collectionText);
      const namespacesParsed = JSON.parse(namespaceText);

      setRemoteCatalogRows(Array.isArray(collectionsParsed) ? collectionsParsed : []);
      setRemoteNamespaces(Array.isArray(namespacesParsed) ? namespacesParsed : []);
    } catch {
      setRemoteCatalogRows([]);
      setRemoteNamespaces([]);
    }
  };

  const runLiveQuery = async () => {
    if (!connected || !liveQuery.trim()) return;

    setLiveQueryLoading(true);
    setLiveQueryError(null);

    try {
      const result = await studioDataClient.executeQuery(liveQuery) as QueryResult;
      setLiveQueryResult(result);
      setLastExecutedQuery(liveQuery);
    } catch (error: unknown) {
      setLiveQueryResult(null);
      setLastExecutedQuery(null);
      setLiveQueryError(error instanceof Error ? error.message : String(error));
    } finally {
      setLiveQueryLoading(false);
    }
  };

  const isShowingCurrentQueryResult =
    liveQueryResult !== null && lastExecutedQuery === liveQuery;

  useEffect(() => {
    void refreshLiveState();
  }, [connected]);

  useEffect(() => {
    if (appTables.length > 0) {
      setLiveTables(appTables);
    }
  }, [appTables]);

  useEffect(() => {
    if (liveTables.length === 0) return;
    const hasBrokenTableReference =
      /FROM\s+LIMIT/i.test(liveQuery) || /FROM\s*$/i.test(liveQuery);

    if (hasBrokenTableReference) {
      updateLiveQuery(`SELECT * FROM ${liveTables[0]} LIMIT 3`);
    }
  }, [liveTables, liveQuery]);

  useEffect(() => {
    if (appStatus) {
      setLiveStats(appStatus);
    }
  }, [appStatus]);

  useEffect(() => {
    if (!isRemoteConnection || liveTables.length === 0) return;
    if (!remoteCatalogTable || !liveTables.includes(remoteCatalogTable)) {
      void loadRemoteCatalogDetail(liveTables[0]);
    }
  }, [isRemoteConnection, liveTables]);

  useEffect(() => {
    if (isRemoteConnection && connected) {
      void loadRemoteCatalog();
    }
  }, [isRemoteConnection, connected, liveTables.join("|")]);

  const filteredRemoteCatalogRows = remoteCatalogRows.filter((row) => {
    const haystack = JSON.stringify(row).toLowerCase();
    return haystack.includes(remoteCatalogFilter.toLowerCase());
  });

  return (
    <div className="h-full overflow-y-auto animate-in p-8">
      <header className="mb-8">
        <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-full bg-teal/10 text-teal text-xs font-semibold uppercase tracking-wider border border-teal/20 mb-4">
          SochDB Studio Workbench
        </div>
        <h1 className="text-3xl font-bold text-text-default mb-2">ML / Retrieval Console</h1>
        <p className="text-text-muted max-w-3xl">
          A product-facing surface for understanding SochDB from the retrieval and vector-search side:
          inspect datasets, indexes, search behavior, and the difference between local embedded mode
          and 2.0 client/server mode.
        </p>
        <p className="text-xs text-text-muted mt-3">
          Snapshot generated from benchmark artifacts at {new Date(workbenchGeneratedAt).toLocaleString()}.
        </p>
      </header>

      <div className="glass-panel rounded-2xl p-3 mb-8 inline-flex gap-3">
        {(["local", "remote"] as WorkbenchMode[]).map((candidate) => (
          <button
            key={candidate}
            onClick={() => setMode(candidate)}
            className={`px-4 py-2 rounded-xl text-sm font-medium transition-colors ${
              mode === candidate
                ? "bg-teal text-white"
                : "bg-background-muted text-text-muted hover:text-text-default"
            }`}
          >
            {workbenchData[candidate].label}
          </button>
        ))}
      </div>

      <div className="glass-panel rounded-2xl p-6 mb-8">
        <SectionHeader
          title="Live Database Connection"
          subtitle={
            connected
              ? "This panel uses the actual Studio backend to inspect the connected database."
              : "Not connected right now. The rest of the Workbench is using generated benchmark snapshots."
          }
          badge={connected ? "Live backend" : "Snapshot only"}
        />

        <div className="flex flex-wrap gap-3 mb-4">
          <div className={`px-3 py-2 rounded-xl border text-sm ${connected ? "border-teal/30 bg-teal/10 text-teal" : "border-border-default bg-background-muted text-text-muted"}`}>
            {connected ? "Connected to live database" : "No live database connection"}
          </div>
          <button
            onClick={() => void refreshLiveState()}
            disabled={!connected || liveLoading}
            className="px-4 py-2 rounded-xl border border-border-default bg-background-muted text-text-default disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {liveLoading ? "Refreshing..." : "Refresh live state"}
          </button>
        </div>

        {connected && isRemoteConnection && (
          <div className="mb-4 rounded-2xl border border-border-info bg-background-info/10 p-4">
            <div className="flex flex-wrap items-center gap-3 mb-3">
              <div className="text-xs uppercase tracking-wider text-text-muted">Remote endpoint</div>
              <div className="px-3 py-1.5 rounded-full border border-border-default bg-background-app text-sm text-text-default font-mono">
                {appConnection?.endpoint || "gRPC"}
              </div>
            </div>
            <div className="flex flex-wrap gap-2 text-xs">
              <span className="px-3 py-1.5 rounded-full border border-border-info bg-background-app text-text-info">
                Remote health + collection discovery enabled
              </span>
              <span className={`px-3 py-1.5 rounded-full border ${appConnection?.features?.query ? "border-teal/30 bg-teal/10 text-teal" : "border-border-default bg-background-app text-text-muted"}`}>
                Remote query {appConnection?.features?.query ? "enabled" : "disabled"}
              </span>
              <span className={`px-3 py-1.5 rounded-full border ${appConnection?.features?.mcp ? "border-teal/30 bg-teal/10 text-teal" : "border-border-default bg-background-app text-text-muted"}`}>
                Full MCP {appConnection?.features?.mcp ? "enabled" : "not yet"}
              </span>
              <span className={`px-3 py-1.5 rounded-full border ${appConnection?.features?.write ? "border-teal/30 bg-teal/10 text-teal" : "border-border-default bg-background-app text-text-muted"}`}>
                Remote writes {appConnection?.features?.write ? "enabled" : "not yet"}
              </span>
            </div>
          </div>
        )}

        {liveError && (
          <div className="rounded-xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-300 mb-4">
            {liveError}
          </div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
          <MetricCard
            label="Live Tables"
            value={liveStats ? String(liveStats.total_tables) : "—"}
            sub={isRemoteConnection ? "Collection count reported by the connected remote instance." : "Count reported by the current embedded connection."}
          />
          <MetricCard
            label="Live Rows"
            value={liveStats ? String(liveStats.total_rows) : "—"}
            sub={isRemoteConnection ? "Aggregate remote document count from collection metadata." : "Top-level rows discovered by the Studio backend scan."}
          />
          {isRemoteConnection && (
            <MetricCard
              label="Namespaces"
              value={liveStats ? String(liveStats.namespace_count ?? remoteNamespaces.length ?? 0) : "—"}
              sub="Namespaces discovered from the connected remote server."
            />
          )}
          {isRemoteConnection && (
            <MetricCard
              label="Health"
              value={String(liveStats?.health_status ?? "UNKNOWN")}
              sub="Server health reported at connection and refresh time."
            />
          )}
          <MetricCard
            label="MemTable"
            value={liveStats ? `${(liveStats.memtable_size_bytes / 1024).toFixed(2)} KB` : "—"}
            sub="Current in-memory write buffer usage."
          />
          <MetricCard
            label="Last Refresh"
            value={lastRefreshAt ? lastRefreshAt.toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" }) : "—"}
            sub="Most recent time this live panel pulled fresh database state."
          />
        </div>

        {connected && (
          <div className="mt-5 rounded-2xl border border-border-default bg-background-muted/40 p-4">
            <div className="text-xs uppercase tracking-wider text-text-muted mb-2">Live tables</div>
            {liveTables.length > 0 ? (
              <div className="flex flex-wrap gap-2">
                {liveTables.map((table) => (
                  <button
                    key={table}
                    onClick={() => {
                      if (isRemoteConnection) {
                        void loadRemoteCatalogDetail(table);
                      } else {
                        updateLiveQuery(`SELECT * FROM ${table} LIMIT 3`);
                      }
                    }}
                    className="px-3 py-1.5 rounded-full border border-border-default bg-background-app text-sm text-text-default"
                  >
                    {table}
                  </button>
                ))}
              </div>
            ) : (
              <p className="text-sm text-text-muted">
                No tables discovered yet. You can create sample data from the Console tab and then refresh this panel.
              </p>
            )}
          </div>
        )}

        {connected && isRemoteConnection && (
          <div className="mt-5 rounded-2xl border border-border-default bg-background-muted/40 p-4">
            <div className="flex items-start justify-between gap-3 mb-3">
              <div>
                <div className="text-xs uppercase tracking-wider text-text-muted mb-1">Remote collection detail</div>
                <div className="text-sm text-text-default font-medium">
                  {remoteCatalogTable || "Choose a collection above"}
                </div>
              </div>
              {remoteCatalogLoading && <div className="text-xs text-text-muted">Loading…</div>}
            </div>

            {remoteCatalogError && (
              <div className="rounded-xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-300 mb-3">
                {remoteCatalogError}
              </div>
            )}

            {remoteCatalogDetail ? (
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
                <MetricCard
                  label="Namespace"
                  value={String(remoteCatalogDetail.namespace ?? "default")}
                  sub="Remote namespace attached to this collection."
                />
                <MetricCard
                  label="Dimension"
                  value={String(remoteCatalogDetail.dimension ?? "—")}
                  sub="Configured embedding/vector dimension."
                />
                <MetricCard
                  label="Metric"
                  value={String(remoteCatalogDetail.metric ?? "—")}
                  sub="Distance metric reported by the server."
                />
                <MetricCard
                  label="Documents"
                  value={String(remoteCatalogDetail.document_count ?? "0")}
                  sub="Document count reported by collection metadata."
                />
              </div>
            ) : (
              <div className="rounded-xl border border-border-default bg-background-app px-4 py-3 text-sm text-text-muted">
                Select a remote collection to inspect its metadata from the server.
              </div>
            )}
          </div>
        )}

        {connected && isRemoteConnection && (
          <div className="mt-5 rounded-2xl border border-border-default bg-background-muted/40 p-4">
            <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-3 mb-4">
              <div>
                <div className="text-xs uppercase tracking-wider text-text-muted mb-1">Remote collections catalog</div>
                <div className="text-sm text-text-default">
                  Inventory view of collections exposed by the connected remote instance.
                </div>
              </div>
              <input
                type="text"
                value={remoteCatalogFilter}
                onChange={(event) => setRemoteCatalogFilter(event.target.value)}
                placeholder="Filter collections or namespaces"
                className="w-full md:w-72 bg-background-app border border-border-input rounded-lg px-4 py-2.5 text-text-default text-sm outline-none placeholder:text-text-muted"
              />
            </div>

            {filteredRemoteCatalogRows.length > 0 ? (
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
                {filteredRemoteCatalogRows.map((row, index) => {
                  const namespace = String(row.namespace ?? "default");
                  const name = String(row.name ?? `collection-${index + 1}`);
                  const fullName = namespace && namespace !== "default" ? `${namespace}:${name}` : name;
                  return (
                    <button
                      key={`${fullName}-${index}`}
                      onClick={() => void loadRemoteCatalogDetail(fullName)}
                      className={`rounded-2xl border p-4 text-left transition-colors ${
                        remoteCatalogTable === fullName
                          ? "border-teal/40 bg-teal/10"
                          : "border-border-default bg-background-app hover:bg-background-muted"
                      }`}
                    >
                      <div className="flex items-start justify-between gap-3 mb-2">
                        <div>
                          <div className="text-sm font-semibold text-text-default">{name}</div>
                          <div className="text-xs text-text-muted font-mono">{namespace}</div>
                        </div>
                        <div className="text-xs px-2 py-1 rounded-full border border-border-default bg-background-muted text-text-muted">
                          {String(row.metric ?? "—")}
                        </div>
                      </div>
                      <div className="grid grid-cols-2 gap-3 text-xs text-text-muted">
                        <div>
                          <div className="uppercase tracking-wider mb-1">Docs</div>
                          <div className="text-sm text-text-default font-semibold">{String(row.document_count ?? 0)}</div>
                        </div>
                        <div>
                          <div className="uppercase tracking-wider mb-1">Dim</div>
                          <div className="text-sm text-text-default font-semibold">{String(row.dimension ?? "—")}</div>
                        </div>
                      </div>
                    </button>
                  );
                })}
              </div>
            ) : (
              <div className="rounded-xl border border-border-default bg-background-app px-4 py-3 text-sm text-text-muted">
                No remote collections matched the current filter.
              </div>
            )}
          </div>
        )}
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 mb-8">
        <div className="xl:col-span-2 glass-panel rounded-2xl p-6">
          <SectionHeader
            title="Overview"
            subtitle="V1 should make the active dataset, vector count, embedding dimension, and quality signals obvious."
            badge={current.modeBadge}
          />
          <div className="grid grid-cols-1 md:grid-cols-2 2xl:grid-cols-4 gap-4">
            <MetricCard label="Dataset" value={current.dataset} sub="Real benchmark corpus, not synthetic placeholder data." />
            <MetricCard label="Vectors" value={current.vectors} sub="Indexed document count for the active SciFact run." />
            <MetricCard label="Queries" value={current.queryCount} sub="Labeled benchmark queries used for evaluation." />
            <MetricCard label="Embedding Dimension" value={current.dimension} sub="The measured SciFact runs use MiniLM embeddings from the benchmark harness." />
            <MetricCard label="recall@5" value={current.recall} sub={current.recallSub} />
          </div>
        </div>

        <div className="glass-panel rounded-2xl p-6">
          <SectionHeader
            title="Index Summary"
            subtitle="This should be the first place users look when asking what vectors are actually stored."
          />
          <div className="space-y-3">
            <div className="rounded-xl bg-background-muted/60 border border-border-default p-4">
              <div className="text-xs uppercase tracking-wider text-text-muted mb-2">Index name</div>
              <div className="text-lg font-semibold text-text-default break-all">{current.indexName}</div>
            </div>
            <div className="rounded-xl bg-background-muted/60 border border-border-default p-4">
              <div className="text-xs uppercase tracking-wider text-text-muted mb-2">Index settings</div>
              <div className="text-lg font-semibold text-text-default">{current.indexSummary}</div>
            </div>
            <div className="rounded-xl bg-background-muted/60 border border-border-default p-4">
              <div className="text-xs uppercase tracking-wider text-text-muted mb-2">Search path</div>
              <div className="text-lg font-semibold text-text-default">{current.searchPath}</div>
            </div>
          </div>
        </div>
      </div>

      <div className="glass-panel rounded-2xl p-6 mb-8">
        <SectionHeader
          title="Live Read Query Demo"
          subtitle={
            isRemoteConnection
              ? "This panel runs the remote-safe query presets we currently support through the Studio backend."
              : "This panel is for read-only queries against the connected database. Use Console for writes like put/insert/create."
          }
          badge={connected ? (isRemoteConnection ? "Remote-safe" : "Read-only") : "Connect to enable"}
        />

        <div className="flex flex-col gap-4">
          <div className="flex flex-wrap gap-2">
            {liveQueryExamples.map((example) => (
              <button
                key={example}
                onClick={() => updateLiveQuery(example)}
                className="px-3 py-1.5 rounded-full border border-border-default bg-background-muted text-xs text-text-default hover:bg-background-app transition-colors"
              >
                {example}
              </button>
            ))}
          </div>

          <div className="flex flex-col xl:flex-row gap-3">
            <input
              type="text"
              value={liveQuery}
              onChange={(event) => updateLiveQuery(event.target.value)}
              placeholder={
                isRemoteConnection
                  ? "Remote-safe examples: SHOW COLLECTIONS, SELECT * FROM collections, SHOW NAMESPACES"
                  : "Read-only examples: SELECT * FROM users LIMIT 3, users, users/1"
              }
              className="flex-1 bg-background-app border border-border-input rounded-lg px-4 py-3 text-text-default text-sm outline-none placeholder:text-text-muted"
            />
            <button
              onClick={() => void runLiveQuery()}
              disabled={!connected || liveQueryLoading || !liveQuery.trim()}
              className="px-5 py-3 rounded-lg bg-teal text-white font-medium disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {liveQueryLoading ? "Running..." : "Run read query"}
            </button>
          </div>

          <p className="text-xs text-text-muted">
            {isRemoteConnection
              ? "Supported here today: collection and namespace discovery queries over gRPC. Not supported yet: remote writes, arbitrary SQL, and full MCP tool parity."
              : "Supported here: read queries and path scans. Not supported here: writes, inserts, schema changes, or sample-data creation."}
          </p>

          {liveQueryError && (
            <div className="rounded-xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-300">
              {liveQueryError}
            </div>
          )}

          {isShowingCurrentQueryResult ? (
            <div className="rounded-2xl border border-border-default bg-background-app overflow-hidden">
              <div className="px-4 py-3 border-b border-border-default text-xs text-text-muted">
                rows {liveQueryResult.stats.row_count} · execution {liveQueryResult.stats.execution_time_ms.toFixed(3)} ms
              </div>
              <div className="overflow-auto">
                <table className="w-full text-left border-collapse">
                  <thead>
                    <tr>
                      {liveQueryResult.columns.map((column, index) => (
                        <th key={index} className="p-3 text-xs font-mono text-text-muted border-b border-border-default">
                          {column}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {liveQueryResult.rows.map((row, rowIndex) => (
                      <tr key={rowIndex} className="hover:bg-white/5">
                        {row.map((value, cellIndex) => (
                          <td key={cellIndex} className="p-3 text-sm text-text-default border-b border-border-default/50 font-mono align-top">
                            {typeof value === "object" ? JSON.stringify(value) : String(value)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ) : (
            <div className="rounded-2xl border border-border-default bg-background-muted/40 p-4 text-sm text-text-muted">
              {connected
                ? lastExecutedQuery && lastExecutedQuery !== liveQuery
                  ? "The query changed. Run it to see fresh results for the current input."
                  : "Run a live query to inspect the connected database from inside the Workbench."
                : "Connect to a database in Studio to use the live query demo."}
            </div>
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 mb-8">
        <div className="xl:col-span-2 glass-panel rounded-2xl p-6">
          <SectionHeader
            title="Search Playground"
            subtitle="Users should be able to inspect a real query and see how local and remote results compare."
            badge="Workbench V1"
          />

          <div className="flex flex-wrap gap-3 mb-4">
            <label className="flex items-center gap-2 px-3 py-2 rounded-xl border border-border-default bg-background-muted text-sm text-text-muted">
              Sample query
              <select
                value={selectedQuery.id}
                onChange={(event) => setSelectedQueryId(event.target.value)}
                className="bg-transparent text-text-default outline-none"
              >
                {current.queries.map((query) => (
                  <option key={query.id} value={query.id}>
                    Query {query.id}
                  </option>
                ))}
              </select>
            </label>
            <div className="px-3 py-2 rounded-xl border border-border-default bg-background-muted text-sm text-text-default">
              p50 {current.latencyP50}
            </div>
            <div className="px-3 py-2 rounded-xl border border-border-default bg-background-muted text-sm text-text-default">
              selected query latency {selectedQuery.latency}
            </div>
          </div>

          <div className="rounded-2xl border border-border-default bg-background-muted/50 p-5 mb-4">
            <div className="text-xs uppercase tracking-wider text-text-muted mb-2">Selected query</div>
            <p className="text-xl font-semibold text-text-default mb-3 leading-relaxed">{selectedQuery.text}</p>
            <div className="flex flex-wrap gap-2">
              <span className="px-3 py-1.5 rounded-full bg-teal/10 text-teal text-xs border border-teal/20">
                {current.modePill}
              </span>
              <span className="px-3 py-1.5 rounded-full bg-blue-500/10 text-text-info text-xs border border-border-info">
                {current.qualityPill}
              </span>
              <span className="px-3 py-1.5 rounded-full bg-orange/10 text-orange text-xs border border-orange/20">
                {current.latencyNote}
              </span>
            </div>
          </div>

          <div className="space-y-3">
            {selectedQuery.results.map((result, index) => (
              <div key={`${selectedQuery.id}-${result.id}`} className="rounded-2xl border border-border-default bg-background-muted/40 p-4">
                <div className="flex items-start justify-between gap-3 mb-2">
                  <div>
                    <div className="text-sm font-semibold text-text-default">Result {index + 1}</div>
                    <div className="text-xs font-mono text-text-muted">doc_id: {result.id}</div>
                  </div>
                  <div className="px-3 py-1 rounded-full border border-border-default text-xs text-text-muted bg-background-app">
                    distance {result.distance}
                  </div>
                </div>
                <p className="text-sm text-text-muted leading-relaxed">{result.note}</p>
              </div>
            ))}
          </div>
        </div>

        <div className="glass-panel rounded-2xl p-6">
          <SectionHeader
            title="Observability Snapshot"
            subtitle="Studio should connect retrieval workflows to operational signal."
          />
          <div className="space-y-3">
            {current.observability.map((item) => (
              <div key={item.label} className="rounded-xl bg-background-muted/60 border border-border-default p-4">
                <div className="flex items-center justify-between gap-3">
                  <span className="text-sm text-text-muted">{item.label}</span>
                  <span className="text-sm font-semibold text-text-default">{item.value}</span>
                </div>
              </div>
            ))}
          </div>
          <p className="text-xs text-text-muted mt-4 leading-relaxed">
            The main repo already includes a Grafana overview dashboard covering request rate, latency,
            active connections, memory usage, vector operations per second, and server status.
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6 mb-8">
        <div className="glass-panel rounded-2xl p-6">
          <SectionHeader
            title="Dataset Explorer"
            subtitle="People need to see what the benchmark data actually looks like, not just the metric line."
          />
          <div className="mb-4">
            <label className="flex items-center gap-2 px-3 py-2 rounded-xl border border-border-default bg-background-muted text-sm text-text-muted w-fit">
              Sample document
              <select
                value={selectedDoc.id}
                onChange={(event) => setSelectedDocId(event.target.value)}
                className="bg-transparent text-text-default outline-none"
              >
                {current.docs.map((doc) => (
                  <option key={doc.id} value={doc.id}>
                    {doc.id}
                  </option>
                ))}
              </select>
            </label>
          </div>

          <div className="space-y-4">
            <div className="rounded-2xl border border-border-default bg-background-muted/40 p-4">
              <div className="text-xs uppercase tracking-wider text-text-muted mb-2">Dataset facts</div>
              <div className="space-y-2 text-sm text-text-muted">
                {current.datasetFacts.map((fact) => (
                  <p key={fact}>{fact}</p>
                ))}
              </div>
            </div>

            <div className="rounded-2xl border border-border-default bg-background-muted/40 p-4">
              <div className="flex items-start justify-between gap-3 mb-2">
                <div>
                  <div className="text-sm font-semibold text-text-default">{selectedDoc.title}</div>
                  <div className="text-xs font-mono text-text-muted">id {selectedDoc.id}</div>
                </div>
              </div>
              <p className="text-sm text-text-muted leading-relaxed">{selectedDoc.body}</p>
            </div>
          </div>
        </div>

        <div className="glass-panel rounded-2xl p-6">
          <SectionHeader
            title="Evidence Panel"
            subtitle="Keep measured results, live observations, and claims separated so the product story stays honest."
          />
          <div className="space-y-3">
            {current.evidence.map((item) => (
              <div key={item.title} className="rounded-2xl border border-border-default bg-background-muted/40 p-4">
                <div className="text-sm font-semibold text-text-default mb-1">{item.title}</div>
                <p className="text-sm text-text-muted">{item.body}</p>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="glass-panel rounded-2xl p-6">
        <SectionHeader
          title="Ingestion / Trace Story"
          subtitle="Studio should eventually make indexing, embedding, and search events visible, not just query output."
        />
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {current.traceSteps.map((step) => (
            <div key={step.title} className="rounded-2xl border border-border-default bg-background-muted/40 p-4">
              <div className="text-sm font-semibold text-text-default mb-2">{step.title}</div>
              <p className="text-sm text-text-muted">{step.body}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
