import { invoke } from '@tauri-apps/api/core';

export type DatabaseStats = {
  memtable_size_bytes: number;
  wal_size_bytes: number;
  total_tables: number;
  total_rows: number;
  namespace_count?: number;
  health_status?: string;
  active_transactions: number;
  last_checkpoint_lsn: number;
  uptime_seconds: number;
  version: string;
  active_snapshots: number;
  min_active_timestamp: number;
  garbage_versions: number;
};

export type ConnectionInfo = {
  path: string;
  connected: boolean;
  version: string;
  uptime_seconds: number;
  instanceType?: string | null;
  endpoint?: string | null;
  projectId?: string | null;
  projectName?: string | null;
  instanceId?: string | null;
  instanceName?: string | null;
  features?: {
    query: boolean;
    mcp: boolean;
    write: boolean;
  };
};

export type StudioInstance = {
  id: string;
  name: string;
  type: 'embedded' | 'remote' | 'managed' | string;
  dbPath?: string | null;
  host?: string | null;
  port?: number | null;
  apiKey?: string | null;
  tls?: boolean;
  createdAt: string;
  lastConnectedAt?: string | null;
};

export type StudioProject = {
  id: string;
  name: string;
  createdAt: string;
  lastConnectedAt?: string | null;
  eventCount?: number;
  lastEventAt?: string | null;
  apiKeys?: StudioApiKey[];
  instances: StudioInstance[];
};

export type StudioWorkspace = {
  id: string;
  name: string;
  projects: StudioProject[];
};

export type StudioApiKey = {
  id: string;
  name: string;
  prefix: string;
  createdAt: string;
  lastUsedAt?: string | null;
};

export type StudioIngestEvent = {
  id: string;
  type: string;
  name: string;
  input?: unknown;
  output?: unknown;
  metadata?: Record<string, unknown>;
  tags?: string[];
  level?: string;
  status?: string;
  source?: string;
  timestamp: string;
};

export type QueryResult = {
  columns: string[];
  rows: unknown[][];
  stats?: {
    row_count: number;
    execution_time_ms: number;
    scanned_rows: number;
  };
};

export type McpCallToolResponse = {
  content?: Array<{
    type?: string;
    text?: string;
  }>;
  isError?: boolean;
};

export interface StudioDataClient {
  readonly mode: 'tauri' | 'http';
  connect(
    path: string,
    options?: {
      projectId?: string;
      projectName?: string;
      instanceId?: string;
      instanceName?: string;
      instanceType?: 'embedded' | 'remote';
      host?: string;
      port?: number;
      apiKey?: string;
      tls?: boolean;
      workspaceId?: string;
    }
  ): Promise<ConnectionInfo>;
  disconnect(): Promise<void>;
  getStats(): Promise<DatabaseStats>;
  executeQuery(query: string): Promise<QueryResult>;
  mcpCallTool(toolName: string, args: Record<string, unknown>): Promise<McpCallToolResponse>;
  listTables(): Promise<string[]>;
  listWorkspaces(): Promise<StudioWorkspace[]>;
  createProject(input: { name: string; dbPath?: string; workspaceId?: string }): Promise<StudioProject>;
  createInstance(input: { projectId: string; name: string; type?: string; dbPath?: string; host?: string; port?: number; apiKey?: string; tls?: boolean; workspaceId?: string }): Promise<StudioInstance>;
  createApiKey(input: { projectId: string; name: string; workspaceId?: string }): Promise<{ apiKey: StudioApiKey; secret: string }>;
  revokeApiKey(input: { projectId: string; apiKeyId: string; workspaceId?: string }): Promise<{ ok: boolean; project: StudioProject }>;
  listEvents(input: { projectId: string; workspaceId?: string; limit?: number }): Promise<{ project: StudioProject; events: StudioIngestEvent[] }>;
  ingestEvents(input: { apiKey: string; source?: string; events: Array<Record<string, unknown>> }): Promise<{ ok: boolean; ingested: number; eventIds: string[] }>;
}

function parseTableNames(content: string): string[] {
  try {
    const parsed = JSON.parse(content);
    if (Array.isArray(parsed)) {
      return parsed
        .map((item) => {
          if (typeof item === 'string') return item;
          if (item && typeof item === 'object' && 'name' in item) {
            return String((item as { name: unknown }).name);
          }
          return '';
        })
        .filter(Boolean);
    }
  } catch {
    // Fall back to plain-text parsing below.
  }

  return content
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => {
      if (!line) return false;
      if (line.startsWith('results[')) return false;
      if (/^\[\d+\]:?$/.test(line)) return false;
      if (line === '[]' || line === '{}') return false;
      return true;
    })
    .map((line) => line.split(',')[0]?.trim())
    .filter((name): name is string => Boolean(name));
}

function normalizeError(error: unknown): Error {
  if (error instanceof Error) return error;
  if (typeof error === 'string') return new Error(error);
  return new Error(JSON.stringify(error));
}

function createTauriStudioDataClient(): StudioDataClient {
  return {
    mode: 'tauri',
    async connect(path) {
      try {
        return await invoke<ConnectionInfo>('connect', { path });
      } catch (error) {
        throw normalizeError(error);
      }
    },
    async disconnect() {
      try {
        await invoke('disconnect');
      } catch (error) {
        throw normalizeError(error);
      }
    },
    async getStats() {
      try {
        return await invoke<DatabaseStats>('get_stats');
      } catch (error) {
        throw normalizeError(error);
      }
    },
    async executeQuery(query) {
      try {
        return await invoke<QueryResult>('execute_query', { query });
      } catch (error) {
        throw normalizeError(error);
      }
    },
    async mcpCallTool(toolName, args) {
      try {
        return await invoke<McpCallToolResponse>('mcp_call_tool', {
          toolName,
          arguments: args,
        });
      } catch (error) {
        throw normalizeError(error);
      }
    },
    async listTables() {
      const response = await this.mcpCallTool('sochdb_list_tables', {});
      const content = response?.content?.[0]?.text ?? '';
      return parseTableNames(content);
    },
    async listWorkspaces() {
      return [{
        id: 'local',
        name: 'Local Workspace',
        projects: [],
      }];
    },
    async createProject(input) {
      return {
        id: 'local-project',
        name: input.name,
        createdAt: new Date().toISOString(),
        lastConnectedAt: null,
        instances: input.dbPath ? [{
          id: 'local-instance',
          name: `${input.name} Primary`,
          type: 'embedded',
          dbPath: input.dbPath,
          host: null,
          port: null,
          apiKey: null,
          tls: false,
          createdAt: new Date().toISOString(),
          lastConnectedAt: null,
        }] : [],
      };
    },
    async createInstance(input) {
      return {
        id: 'local-instance',
        name: input.name,
        type: input.type || 'embedded',
        dbPath: input.dbPath || null,
        host: input.host || null,
        port: input.port || null,
        apiKey: input.apiKey || null,
        tls: Boolean(input.tls),
        createdAt: new Date().toISOString(),
        lastConnectedAt: null,
      };
    },
    async createApiKey() {
      throw new Error('Project API keys are available in Studio web mode');
    },
    async revokeApiKey() {
      throw new Error('Project API key revocation is available in Studio web mode');
    },
    async listEvents() {
      return {
        project: {
          id: 'local-project',
          name: 'Local Project',
          createdAt: new Date().toISOString(),
          instances: [],
        },
        events: [],
      };
    },
    async ingestEvents() {
      throw new Error('Event ingestion is available in Studio web mode');
    },
  };
}

function createHttpStudioDataClient(baseUrl: string): StudioDataClient {
  const request = async <T>(path: string, init?: RequestInit): Promise<T> => {
    const response = await fetch(`${baseUrl}${path}`, {
      headers: {
        'Content-Type': 'application/json',
      },
      ...init,
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    }

    return response.json() as Promise<T>;
  };

  return {
    mode: 'http',
    connect(path, options = {}) {
      return request<ConnectionInfo>('/api/studio/connect', {
        method: 'POST',
        body: JSON.stringify({
          path,
          dbPath: path,
          projectId: options.projectId,
          projectName: options.projectName,
          instanceId: options.instanceId,
          instanceName: options.instanceName,
          instanceType: options.instanceType,
          host: options.host,
          port: options.port,
          apiKey: options.apiKey,
          tls: options.tls,
          workspaceId: options.workspaceId,
        }),
      });
    },
    disconnect() {
      return request<void>('/api/studio/disconnect', {
        method: 'POST',
      });
    },
    getStats() {
      return request<DatabaseStats>('/api/studio/stats');
    },
    executeQuery(query) {
      return request<QueryResult>('/api/studio/query', {
        method: 'POST',
        body: JSON.stringify({ query }),
      });
    },
    mcpCallTool(toolName, args) {
      return request<McpCallToolResponse>('/api/studio/mcp/call', {
        method: 'POST',
        body: JSON.stringify({ toolName, arguments: args }),
      });
    },
    async listTables() {
      const response = await request<{ tables: string[] }>('/api/studio/tables');
      return response.tables;
    },
    async listWorkspaces() {
      const response = await request<{ workspaces: StudioWorkspace[] }>('/api/studio/workspaces');
      return response.workspaces;
    },
    async createProject(input) {
      const response = await request<{ project: StudioProject }>('/api/studio/projects', {
        method: 'POST',
        body: JSON.stringify(input),
      });
      return response.project;
    },
    async createInstance(input) {
      const response = await request<{ instance: StudioInstance }>('/api/studio/instances', {
        method: 'POST',
        body: JSON.stringify(input),
      });
      return response.instance;
    },
    createApiKey(input) {
      return request<{ apiKey: StudioApiKey; secret: string }>('/api/studio/api-keys', {
        method: 'POST',
        body: JSON.stringify(input),
      });
    },
    revokeApiKey(input) {
      return request<{ ok: boolean; project: StudioProject }>('/api/studio/api-keys', {
        method: 'DELETE',
        body: JSON.stringify(input),
      });
    },
    listEvents(input) {
      const params = new URLSearchParams({
        projectId: input.projectId,
        workspaceId: input.workspaceId || 'default',
        limit: String(input.limit || 100),
      });
      return request<{ project: StudioProject; events: StudioIngestEvent[] }>(`/api/studio/events?${params.toString()}`);
    },
    ingestEvents(input) {
      return request<{ ok: boolean; ingested: number; eventIds: string[] }>('/api/studio/ingest/events', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${input.apiKey}`,
        },
        body: JSON.stringify({
          source: input.source || 'sdk',
          events: input.events,
        }),
      });
    },
  };
}

function resolveStudioDataClient(): StudioDataClient {
  const webBaseUrl = (import.meta.env.VITE_STUDIO_API_BASE_URL as string | undefined)?.trim();
  if (webBaseUrl) {
    return createHttpStudioDataClient(webBaseUrl.replace(/\/$/, ''));
  }
  const isTauriRuntime =
    typeof window !== 'undefined' && Boolean((window as unknown as { __TAURI_INTERNALS__?: unknown }).__TAURI_INTERNALS__);
  if (typeof window !== 'undefined' && !isTauriRuntime) {
    return createHttpStudioDataClient('');
  }
  return createTauriStudioDataClient();
}

export const studioDataClient = resolveStudioDataClient();
export const studioClientMode = studioDataClient.mode;
export const studioBackendBaseUrl =
  ((import.meta.env.VITE_STUDIO_API_BASE_URL as string | undefined)?.trim() || null);
