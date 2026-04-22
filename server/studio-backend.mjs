import http from 'node:http';
import { URL } from 'node:url';
import { McpClient } from './mcpClient.mjs';
import { GrpcClient } from './grpcClient.mjs';
import {
  authenticateApiKey,
  createInstance,
  createApiKey,
  createProject,
  getInstance,
  getProject,
  getStatePath,
  ingestEvents,
  listProjectEvents,
  listWorkspaces,
  markInstanceConnected,
  markProjectConnected,
  revokeApiKey,
} from './projectStore.mjs';

const port = Number(process.env.STUDIO_BACKEND_PORT || 4318);

let currentSession = null;

function json(res, statusCode, body) {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  });
  res.end(JSON.stringify(body));
}

function parseToolText(response) {
  return response?.content?.[0]?.text ?? '';
}

function parseTableNames(content) {
  try {
    const parsed = JSON.parse(content);
    if (Array.isArray(parsed)) {
      return parsed
        .map((item) => {
          if (typeof item === 'string') return item;
          if (item && typeof item === 'object' && 'name' in item) {
            return String(item.name);
          }
          return '';
        })
        .filter(Boolean);
    }
  } catch {
    // fall through
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
    .filter(Boolean);
}

async function parseBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }
  const raw = Buffer.concat(chunks).toString('utf8');
  return raw ? JSON.parse(raw) : {};
}

function parseBearerToken(req) {
  const authorization = req.headers.authorization;
  if (typeof authorization === 'string' && authorization.startsWith('Bearer ')) {
    return authorization.slice('Bearer '.length).trim();
  }

  const apiKey = req.headers['x-api-key'];
  if (typeof apiKey === 'string') {
    return apiKey.trim();
  }

  return null;
}

function requireSession() {
  if (!currentSession) {
    throw new Error('No active Studio backend connection');
  }
  return currentSession;
}

async function getTables(session) {
  if (session.mode === 'remote') {
    return session.client.listTables();
  }

  const result = await session.client.callTool('sochdb_list_tables', {});
  const content = parseToolText(result);
  return parseTableNames(content);
}

async function getStats(session) {
  if (session.mode === 'remote') {
    return session.client.getStats();
  }

  const tables = await getTables(session);
  let totalRows = 0;

  for (const table of tables) {
    try {
      const result = await session.client.callTool('sochdb_query', {
        query: `SELECT * FROM ${table}`,
        format: 'json',
      });
      const content = parseToolText(result);
      const parsed = JSON.parse(content);
      if (Array.isArray(parsed)) {
        totalRows += parsed.length;
      }
    } catch {
      // Best effort for now; later we should expose stats directly from a real backend service.
    }
  }

  return {
    memtable_size_bytes: 0,
    wal_size_bytes: 0,
    total_tables: tables.length,
    total_rows: totalRows,
    active_transactions: 0,
    last_checkpoint_lsn: 0,
    uptime_seconds: Math.floor((Date.now() - session.connectedAt) / 1000),
    version: session.client.serverInfo?.version || 'unknown',
    active_snapshots: 0,
    min_active_timestamp: 0,
    garbage_versions: 0,
  };
}

async function executeQuery(session, query) {
  if (session.mode === 'remote') {
    return session.client.executeQuery(query);
  }

  const result = await session.client.callTool('sochdb_query', {
    query,
    format: 'json',
  });
  const content = parseToolText(result);
  const parsed = JSON.parse(content);

  if (Array.isArray(parsed)) {
    const columns = parsed.length > 0 && typeof parsed[0] === 'object' && parsed[0] !== null
      ? Object.keys(parsed[0])
      : ['value'];
    const rows = parsed.map((row) =>
      columns.map((column) => (row && typeof row === 'object' ? row[column] : row))
    );
    return {
      columns,
      rows,
      stats: {
        row_count: rows.length,
        execution_time_ms: 0,
        scanned_rows: rows.length,
      },
    };
  }

  return {
    columns: ['result'],
    rows: [[parsed]],
    stats: {
      row_count: 1,
      execution_time_ms: 0,
      scanned_rows: 1,
    },
  };
}

async function callTool(session, toolName, args = {}) {
  if (session.mode === 'remote') {
    return session.client.callTool(toolName, args);
  }
  return session.client.callTool(toolName, args);
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);

  if (req.method === 'OPTIONS') {
    json(res, 204, {});
    return;
  }

  try {
    if (req.method === 'GET' && url.pathname === '/health') {
      json(res, 200, {
        ok: true,
        connected: Boolean(currentSession),
        mode: 'http',
      });
      return;
    }

    if (req.method === 'GET' && url.pathname === '/api/studio/health') {
      json(res, 200, {
        ok: true,
        connected: Boolean(currentSession),
        statePath: getStatePath(),
        workspaces: listWorkspaces(),
        session: currentSession
          ? {
              mode: currentSession.mode,
              dbPath: currentSession.dbPath,
              endpoint: currentSession.endpoint || null,
              projectId: currentSession.projectId || null,
              projectName: currentSession.projectName || null,
              serverInfo: currentSession.client.serverInfo,
            }
          : null,
      });
      return;
    }

    if (req.method === 'POST' && url.pathname === '/api/studio/connect') {
      const body = await parseBody(req);
      let projectId = body.projectId || null;
      let projectName = body.projectName || null;
      let instanceId = body.instanceId || null;
      let instanceName = body.instanceName || null;
      let instanceType = body.instanceType || 'embedded';
      let dbPath = body.path || body.dbPath;
      let remoteHost = body.host || null;
      let remotePort = body.port || null;
      let remoteApiKey = body.apiKey || null;
      let remoteTls = Boolean(body.tls);

      if (instanceId) {
        const resolved = getInstance(instanceId, body.workspaceId || 'default');
        projectId = resolved.project.id;
        projectName = resolved.project.name;
        instanceName = resolved.instance.name;
        instanceType = resolved.instance.type || 'embedded';
        dbPath = resolved.instance.dbPath;
        remoteHost = resolved.instance.host || null;
        remotePort = resolved.instance.port || null;
        remoteApiKey = resolved.instance.apiKey || null;
        remoteTls = Boolean(resolved.instance.tls);
      } else if (projectId) {
        const project = getProject(projectId, body.workspaceId || 'default');
        projectName = project.name;
        if (instanceName && (instanceType === 'remote' ? remoteHost : dbPath)) {
          const createdInstance = createInstance({
            workspaceId: body.workspaceId || 'default',
            projectId,
            name: instanceName,
            type: instanceType,
            dbPath,
            host: remoteHost,
            port: remotePort,
            apiKey: remoteApiKey,
            tls: remoteTls,
          });
          instanceId = createdInstance.id;
          instanceName = createdInstance.name;
          instanceType = createdInstance.type || instanceType;
          dbPath = createdInstance.dbPath;
          remoteHost = createdInstance.host || remoteHost;
          remotePort = createdInstance.port ?? remotePort;
          remoteApiKey = createdInstance.apiKey || remoteApiKey;
          remoteTls = Boolean(createdInstance.tls);
        } else {
          const primaryInstance = (project.instances || [])[0];
          if (primaryInstance) {
            instanceId = primaryInstance.id;
            instanceName = primaryInstance.name;
            instanceType = primaryInstance.type || instanceType;
            dbPath = primaryInstance.dbPath;
            remoteHost = primaryInstance.host || null;
            remotePort = primaryInstance.port || null;
            remoteApiKey = primaryInstance.apiKey || null;
            remoteTls = Boolean(primaryInstance.tls);
          }
        }
      } else if (projectName) {
        const project = createProject({
          workspaceId: body.workspaceId || 'default',
          name: projectName,
          dbPath: instanceType === 'embedded' ? dbPath : null,
        });
        projectId = project.id;
        projectName = project.name;
        if (instanceName && (instanceType === 'remote' ? remoteHost : dbPath)) {
          const createdInstance = createInstance({
            workspaceId: body.workspaceId || 'default',
            projectId,
            name: instanceName,
            type: instanceType,
            dbPath,
            host: remoteHost,
            port: remotePort,
            apiKey: remoteApiKey,
            tls: remoteTls,
          });
          instanceId = createdInstance.id;
          instanceName = createdInstance.name;
          instanceType = createdInstance.type || instanceType;
          dbPath = createdInstance.dbPath;
          remoteHost = createdInstance.host || remoteHost;
          remotePort = createdInstance.port ?? remotePort;
          remoteApiKey = createdInstance.apiKey || remoteApiKey;
          remoteTls = Boolean(createdInstance.tls);
        } else {
          const primaryInstance = (project.instances || [])[0];
          if (primaryInstance) {
            instanceId = primaryInstance.id;
            instanceName = primaryInstance.name;
            instanceType = primaryInstance.type || instanceType;
            dbPath = primaryInstance.dbPath;
            remoteHost = primaryInstance.host || null;
            remotePort = primaryInstance.port || null;
            remoteApiKey = primaryInstance.apiKey || null;
            remoteTls = Boolean(primaryInstance.tls);
          }
        }
      }

      const isRemote = instanceType === 'remote';
      if (!isRemote && !dbPath) {
        json(res, 400, { error: 'Missing path' });
        return;
      }
      if (isRemote && !remoteHost) {
        json(res, 400, { error: 'Missing remote host' });
        return;
      }

      if (currentSession) {
        await currentSession.client.stop();
      }

      const client = isRemote
        ? new GrpcClient({
            host: remoteHost,
            port: remotePort || 50051,
            apiKey: remoteApiKey,
            tls: remoteTls,
          })
        : new McpClient(dbPath);
      await client.start();

      currentSession = {
        mode: isRemote ? 'remote' : 'embedded',
        dbPath: isRemote ? null : dbPath,
        endpoint: isRemote ? `${remoteTls ? 'grpcs' : 'grpc'}://${remoteHost}:${remotePort || 50051}` : null,
        projectId,
        projectName,
        instanceId,
        instanceName,
        instanceType,
        connectedAt: Date.now(),
        client,
      };

      if (projectId) {
        markProjectConnected(projectId, body.workspaceId || 'default');
      }
      if (instanceId) {
        markInstanceConnected(instanceId, body.workspaceId || 'default');
      }

      json(res, 200, {
        path: isRemote ? `${remoteHost}:${remotePort || 50051}` : dbPath,
        connected: true,
        instanceType,
        endpoint: isRemote ? currentSession.endpoint : null,
        projectId,
        projectName,
        instanceId,
        instanceName,
        version: client.serverInfo?.version || 'unknown',
        uptime_seconds: 0,
        features: isRemote
          ? { query: true, mcp: false, write: false }
          : { query: true, mcp: true, write: true },
      });
      return;
    }

    if (req.method === 'GET' && url.pathname === '/api/studio/workspaces') {
      json(res, 200, { workspaces: listWorkspaces() });
      return;
    }

    if (req.method === 'POST' && url.pathname === '/api/studio/projects') {
      const body = await parseBody(req);
      const project = createProject({
        workspaceId: body.workspaceId || 'default',
        name: body.name,
        dbPath: body.dbPath || body.path,
      });
      json(res, 200, { project });
      return;
    }

    if (req.method === 'POST' && url.pathname === '/api/studio/api-keys') {
      const body = await parseBody(req);
      const result = createApiKey({
        workspaceId: body.workspaceId || 'default',
        projectId: body.projectId,
        name: body.name,
      });
      json(res, 200, result);
      return;
    }

    if (req.method === 'DELETE' && url.pathname === '/api/studio/api-keys') {
      const body = await parseBody(req);
      const result = revokeApiKey({
        workspaceId: body.workspaceId || 'default',
        projectId: body.projectId,
        apiKeyId: body.apiKeyId,
      });
      json(res, 200, result);
      return;
    }

    if (req.method === 'POST' && url.pathname === '/api/studio/instances') {
      const body = await parseBody(req);
      const instance = createInstance({
        workspaceId: body.workspaceId || 'default',
        projectId: body.projectId,
        name: body.name,
        type: body.type || 'embedded',
        dbPath: body.dbPath || body.path || null,
        host: body.host || null,
        port: body.port || null,
        apiKey: body.apiKey || null,
        tls: Boolean(body.tls),
      });
      json(res, 200, { instance });
      return;
    }

    if (req.method === 'GET' && url.pathname === '/api/studio/events') {
      const projectId = url.searchParams.get('projectId');
      const workspaceId = url.searchParams.get('workspaceId') || 'default';
      const limit = Number(url.searchParams.get('limit') || '100');
      const result = listProjectEvents({
        workspaceId,
        projectId,
        limit,
      });
      json(res, 200, result);
      return;
    }

    if (req.method === 'POST' && url.pathname === '/api/studio/ingest/events') {
      const token = parseBearerToken(req);
      const auth = authenticateApiKey(token || '');
      if (!auth) {
        json(res, 401, { error: 'Invalid or missing API key' });
        return;
      }

      const body = await parseBody(req);
      const payload = Array.isArray(body?.events) ? body.events : Array.isArray(body) ? body : [body];
      const result = ingestEvents({
        workspaceId: auth.workspace.id,
        projectId: auth.project.id,
        apiKeyId: auth.apiKey.id,
        source: body?.source || 'sdk',
        events: payload.filter(Boolean),
      });

      json(res, 200, {
        ok: true,
        workspace: auth.workspace,
        project: result.project,
        ingested: result.ingested,
        eventIds: result.events.map((event) => event.id),
      });
      return;
    }

    if (req.method === 'POST' && url.pathname === '/api/studio/disconnect') {
      if (currentSession) {
        await currentSession.client.stop();
        currentSession = null;
      }
      json(res, 200, { ok: true });
      return;
    }

    if (req.method === 'GET' && url.pathname === '/api/studio/stats') {
      const session = requireSession();
      json(res, 200, await getStats(session));
      return;
    }

    if (req.method === 'GET' && url.pathname === '/api/studio/tables') {
      const session = requireSession();
      json(res, 200, { tables: await getTables(session) });
      return;
    }

    if (req.method === 'POST' && url.pathname === '/api/studio/query') {
      const session = requireSession();
      const body = await parseBody(req);
      if (!body.query) {
        json(res, 400, { error: 'Missing query' });
        return;
      }
      json(res, 200, await executeQuery(session, body.query));
      return;
    }

    if (req.method === 'POST' && url.pathname === '/api/studio/mcp/call') {
      const session = requireSession();
      const body = await parseBody(req);
      if (!body.toolName) {
        json(res, 400, { error: 'Missing toolName' });
        return;
      }
      const result = await callTool(session, body.toolName, body.arguments || {});
      json(res, 200, result);
      return;
    }

    if (req.method === 'GET' && url.pathname === '/api/studio/debug/session') {
      json(res, 200, currentSession?.client.getDiagnostics() || { connected: false });
      return;
    }

    json(res, 404, { error: `Not found: ${url.pathname}` });
  } catch (error) {
    json(res, 500, {
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

server.listen(port, () => {
  console.log(`SochDB Studio backend listening on http://127.0.0.1:${port}`);
});
