import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

const statePath = process.env.STUDIO_BACKEND_STATE_PATH
  ? path.resolve(process.env.STUDIO_BACKEND_STATE_PATH)
  : path.resolve(process.cwd(), 'server-data', 'studio-state.json');

function ensureStateDir() {
  fs.mkdirSync(path.dirname(statePath), { recursive: true });
}

function createDefaultState() {
  return {
    version: 2,
    workspaces: [
      {
        id: 'default',
        name: 'Default Workspace',
        projects: [],
      },
    ],
  };
}

function readState() {
  ensureStateDir();
  if (!fs.existsSync(statePath)) {
    const initial = createDefaultState();
    fs.writeFileSync(statePath, JSON.stringify(initial, null, 2));
    return initial;
  }

  try {
    const parsed = JSON.parse(fs.readFileSync(statePath, 'utf8'));
    for (const workspace of parsed.workspaces || []) {
      for (const project of workspace.projects || []) {
        project.instances ||= [];
        project.apiKeys ||= [];
        project.events ||= [];
      }
    }
    return parsed;
  } catch {
    const fallback = createDefaultState();
    fs.writeFileSync(statePath, JSON.stringify(fallback, null, 2));
    return fallback;
  }
}

function writeState(state) {
  ensureStateDir();
  fs.writeFileSync(statePath, JSON.stringify(state, null, 2));
}

function getWorkspace(state, workspaceId = 'default') {
  const workspace = state.workspaces.find((item) => item.id === workspaceId);
  if (!workspace) {
    throw new Error(`Workspace not found: ${workspaceId}`);
  }
  return workspace;
}

function toApiKeyPrefix(token) {
  return token.slice(0, 12);
}

function hashApiKey(token) {
  return crypto.createHash('sha256').update(token).digest('hex');
}

function serializeProject(project) {
  return {
    id: project.id,
    name: project.name,
    lastConnectedAt: project.lastConnectedAt || null,
    createdAt: project.createdAt,
    eventCount: (project.events || []).length,
    lastEventAt: (project.events || []).at(-1)?.timestamp || null,
    apiKeys: (project.apiKeys || []).map((key) => ({
      id: key.id,
      name: key.name,
      prefix: key.prefix,
      createdAt: key.createdAt,
      lastUsedAt: key.lastUsedAt || null,
    })),
    instances: (project.instances || []).map((instance) => ({
      id: instance.id,
      name: instance.name,
      type: instance.type,
      dbPath: instance.dbPath || null,
      host: instance.host || null,
      port: instance.port || null,
      apiKey: instance.apiKey || null,
      tls: Boolean(instance.tls),
      createdAt: instance.createdAt,
      lastConnectedAt: instance.lastConnectedAt || null,
    })),
  };
}

export function listWorkspaces() {
  const state = readState();
  return state.workspaces.map((workspace) => ({
    id: workspace.id,
    name: workspace.name,
    projects: workspace.projects.map(serializeProject),
  }));
}

export function createProject({ workspaceId = 'default', name, dbPath = null }) {
  if (!name?.trim()) {
    throw new Error('Project name is required');
  }

  const state = readState();
  const workspace = getWorkspace(state, workspaceId);

  const existing = workspace.projects.find(
    (project) => project.name.toLowerCase() === name.trim().toLowerCase()
  );
  if (existing) {
    return existing;
  }

  const project = {
    id: crypto.randomUUID(),
    name: name.trim(),
    createdAt: new Date().toISOString(),
    lastConnectedAt: null,
    instances: [],
    apiKeys: [],
    events: [],
  };

  if (dbPath?.trim()) {
    project.instances.push({
      id: crypto.randomUUID(),
      name: `${project.name} Primary`,
      type: 'embedded',
      dbPath: dbPath.trim(),
      host: null,
      port: null,
      apiKey: null,
      tls: false,
      createdAt: new Date().toISOString(),
      lastConnectedAt: null,
    });
  }

  workspace.projects.push(project);
  writeState(state);
  return project;
}

export function getProject(projectId, workspaceId = 'default') {
  const state = readState();
  const workspace = getWorkspace(state, workspaceId);
  const project = workspace.projects.find((item) => item.id === projectId);
  if (!project) {
    throw new Error(`Project not found: ${projectId}`);
  }
  return project;
}

export function markProjectConnected(projectId, workspaceId = 'default') {
  const state = readState();
  const workspace = getWorkspace(state, workspaceId);
  const project = workspace.projects.find((item) => item.id === projectId);
  if (!project) {
    throw new Error(`Project not found: ${projectId}`);
  }
  project.lastConnectedAt = new Date().toISOString();
  writeState(state);
  return project;
}

export function createInstance({
  workspaceId = 'default',
  projectId,
  name,
  type = 'embedded',
  dbPath = null,
  host = null,
  port = null,
  apiKey = null,
  tls = false,
}) {
  if (!projectId) {
    throw new Error('Project id is required');
  }
  if (!name?.trim()) {
    throw new Error('Instance name is required');
  }

  if (type === 'embedded' && !dbPath?.trim()) {
    throw new Error('Database path is required for embedded instances');
  }

  if (type === 'remote' && !host?.trim()) {
    throw new Error('Host is required for remote instances');
  }

  const state = readState();
  const workspace = getWorkspace(state, workspaceId);
  const project = workspace.projects.find((item) => item.id === projectId);
  if (!project) {
    throw new Error(`Project not found: ${projectId}`);
  }

  project.instances ||= [];

  const existing = project.instances.find(
    (instance) => instance.name.toLowerCase() === name.trim().toLowerCase()
  );
  if (existing) {
    return existing;
  }

  const instance = {
    id: crypto.randomUUID(),
    name: name.trim(),
    type,
    dbPath: dbPath?.trim() || null,
    host: host?.trim() || null,
    port: port ?? null,
    apiKey: apiKey?.trim() || null,
    tls: Boolean(tls),
    createdAt: new Date().toISOString(),
    lastConnectedAt: null,
  };

  project.instances.push(instance);
  writeState(state);
  return instance;
}

export function getInstance(instanceId, workspaceId = 'default') {
  const state = readState();
  const workspace = getWorkspace(state, workspaceId);
  for (const project of workspace.projects) {
    const found = (project.instances || []).find((instance) => instance.id === instanceId);
    if (found) {
      return {
        project,
        instance: found,
      };
    }
  }
  throw new Error(`Instance not found: ${instanceId}`);
}

export function markInstanceConnected(instanceId, workspaceId = 'default') {
  const state = readState();
  const workspace = getWorkspace(state, workspaceId);
  for (const project of workspace.projects) {
    const instance = (project.instances || []).find((item) => item.id === instanceId);
    if (instance) {
      const now = new Date().toISOString();
      instance.lastConnectedAt = now;
      project.lastConnectedAt = now;
      writeState(state);
      return { project, instance };
    }
  }
  throw new Error(`Instance not found: ${instanceId}`);
}

export function getStatePath() {
  return statePath;
}

export function createApiKey({
  workspaceId = 'default',
  projectId,
  name,
}) {
  if (!projectId) {
    throw new Error('Project id is required');
  }
  if (!name?.trim()) {
    throw new Error('API key name is required');
  }

  const state = readState();
  const workspace = getWorkspace(state, workspaceId);
  const project = workspace.projects.find((item) => item.id === projectId);
  if (!project) {
    throw new Error(`Project not found: ${projectId}`);
  }

  const token = `soch_sk_${crypto.randomBytes(24).toString('hex')}`;
  const apiKey = {
    id: crypto.randomUUID(),
    name: name.trim(),
    prefix: toApiKeyPrefix(token),
    hash: hashApiKey(token),
    createdAt: new Date().toISOString(),
    lastUsedAt: null,
  };

  project.apiKeys ||= [];
  project.apiKeys.push(apiKey);
  writeState(state);

  return {
    apiKey: {
      id: apiKey.id,
      name: apiKey.name,
      prefix: apiKey.prefix,
      createdAt: apiKey.createdAt,
      lastUsedAt: apiKey.lastUsedAt,
    },
    secret: token,
  };
}

export function authenticateApiKey(token) {
  if (!token?.trim()) {
    return null;
  }

  const tokenHash = hashApiKey(token.trim());
  const state = readState();

  for (const workspace of state.workspaces || []) {
    for (const project of workspace.projects || []) {
      const apiKey = (project.apiKeys || []).find((item) => item.hash === tokenHash);
      if (apiKey) {
        apiKey.lastUsedAt = new Date().toISOString();
        writeState(state);
        return {
          workspace: { id: workspace.id, name: workspace.name },
          project: serializeProject(project),
          apiKey: {
            id: apiKey.id,
            name: apiKey.name,
            prefix: apiKey.prefix,
            createdAt: apiKey.createdAt,
            lastUsedAt: apiKey.lastUsedAt,
          },
        };
      }
    }
  }

  return null;
}

export function ingestEvents({
  workspaceId = 'default',
  projectId,
  apiKeyId = null,
  source = 'sdk',
  events = [],
}) {
  if (!projectId) {
    throw new Error('Project id is required');
  }

  const state = readState();
  const workspace = getWorkspace(state, workspaceId);
  const project = workspace.projects.find((item) => item.id === projectId);
  if (!project) {
    throw new Error(`Project not found: ${projectId}`);
  }

  project.events ||= [];
  const now = new Date().toISOString();

  const normalized = events.map((event) => ({
    id: crypto.randomUUID(),
    type: typeof event?.type === 'string' ? event.type : 'event',
    name: typeof event?.name === 'string' ? event.name : 'unnamed',
    input: event?.input ?? null,
    output: event?.output ?? null,
    metadata: event?.metadata ?? {},
    tags: Array.isArray(event?.tags) ? event.tags : [],
    level: typeof event?.level === 'string' ? event.level : 'info',
    status: typeof event?.status === 'string' ? event.status : 'recorded',
    source,
    apiKeyId,
    timestamp: typeof event?.timestamp === 'string' ? event.timestamp : now,
  }));

  project.events.push(...normalized);
  if (project.events.length > 5000) {
    project.events = project.events.slice(-5000);
  }

  writeState(state);

  return {
    ingested: normalized.length,
    project: serializeProject(project),
    events: normalized,
  };
}

export function listProjectEvents({
  workspaceId = 'default',
  projectId,
  limit = 100,
}) {
  if (!projectId) {
    throw new Error('Project id is required');
  }

  const state = readState();
  const workspace = getWorkspace(state, workspaceId);
  const project = workspace.projects.find((item) => item.id === projectId);
  if (!project) {
    throw new Error(`Project not found: ${projectId}`);
  }

  const events = (project.events || [])
    .slice(-Math.max(1, Math.min(limit, 500)))
    .reverse();

  return {
    project: serializeProject(project),
    events,
  };
}
