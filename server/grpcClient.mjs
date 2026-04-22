import path from 'node:path';
import { fileURLToPath } from 'node:url';
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROTO_PATH = path.resolve(__dirname, './proto/sochdb.proto');

const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const protoDescriptor = grpc.loadPackageDefinition(packageDef);
const sochdb = protoDescriptor.sochdb?.v1;

if (!sochdb) {
  throw new Error(`Failed to load SochDB gRPC proto from ${PROTO_PATH}`);
}

function unary(stub, method, request, metadata) {
  return new Promise((resolve, reject) => {
    stub[method](request, metadata, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
}

function parseSelectCollectionQuery(query) {
  const trimmed = query.trim().replace(/;$/, '');
  if (/^show\s+collections$/i.test(trimmed) || /^select\s+\*\s+from\s+collections$/i.test(trimmed)) {
    return { kind: 'collections' };
  }
  if (/^show\s+namespaces$/i.test(trimmed) || /^select\s+\*\s+from\s+namespaces$/i.test(trimmed)) {
    return { kind: 'namespaces' };
  }
  return null;
}

export class GrpcClient {
  constructor({ host, port = 50051, apiKey = null, tls = false }) {
    this.host = host;
    this.port = port || 50051;
    this.address = `${host}:${this.port}`;
    this.apiKey = apiKey;
    this.tls = Boolean(tls);
    this.metadata = new grpc.Metadata();
    if (apiKey) {
      this.metadata.set('x-api-key', apiKey);
    }

    const credentials = this.tls
      ? grpc.credentials.createSsl()
      : grpc.credentials.createInsecure();

    this.vectorStub = new sochdb.VectorIndexService(this.address, credentials);
    this.collectionStub = new sochdb.CollectionService(this.address, credentials);
    this.namespaceStub = new sochdb.NamespaceService(this.address, credentials);
    this.serverInfo = null;
    this.connectedAt = null;
  }

  async start() {
    const health = await unary(this.vectorStub, 'HealthCheck', { index_name: '' }, this.metadata);
    this.serverInfo = {
      version: health.version || 'unknown',
      indexes: health.indexes || [],
      healthStatus: health.status,
    };
    this.connectedAt = Date.now();
    return this.serverInfo;
  }

  async stop() {
    this.vectorStub.close();
    this.collectionStub.close();
    this.namespaceStub.close();
  }

  async listNamespaces() {
    const response = await unary(this.namespaceStub, 'ListNamespaces', {}, this.metadata);
    return response.namespaces || [];
  }

  async listCollections(namespace = '') {
    const response = await unary(this.collectionStub, 'ListCollections', { namespace }, this.metadata);
    return response.collections || [];
  }

  async getStats() {
    let namespaces = [];
    try {
      namespaces = await this.listNamespaces();
    } catch {
      namespaces = [];
    }

    const namespaceNames = namespaces.length > 0
      ? namespaces.map((ns) => ns.name).filter(Boolean)
      : ['default'];

    const collectionsByNamespace = await Promise.all(
      namespaceNames.map(async (namespace) => {
        try {
          const collections = await this.listCollections(namespace);
          return collections.map((collection) => ({
            namespace,
            ...collection,
          }));
        } catch {
          return [];
        }
      })
    );

    const collections = collectionsByNamespace.flat();
    const totalRows = collections.reduce((sum, collection) => sum + Number(collection.document_count || 0), 0);

    return {
      memtable_size_bytes: 0,
      wal_size_bytes: 0,
      total_tables: collections.length,
      total_rows: totalRows,
      namespace_count: namespaceNames.length,
      health_status: this.serverInfo?.healthStatus || 'UNKNOWN',
      active_transactions: 0,
      last_checkpoint_lsn: 0,
      uptime_seconds: this.connectedAt ? Math.floor((Date.now() - this.connectedAt) / 1000) : 0,
      version: this.serverInfo?.version || 'unknown',
      active_snapshots: 0,
      min_active_timestamp: 0,
      garbage_versions: 0,
    };
  }

  async listTables() {
    const namespaces = await this.listNamespaces().catch(() => []);
    const namespaceNames = namespaces.length > 0
      ? namespaces.map((ns) => ns.name).filter(Boolean)
      : ['default'];

    const collectionsByNamespace = await Promise.all(
      namespaceNames.map(async (namespace) => {
        const collections = await this.listCollections(namespace).catch(() => []);
        return collections.map((collection) =>
          namespace && namespace !== 'default'
            ? `${namespace}:${collection.name}`
            : collection.name
        );
      })
    );

    return collectionsByNamespace.flat();
  }

  async describeTable(tableName) {
    const [namespace, collectionName] = tableName.includes(':')
      ? tableName.split(':', 2)
      : ['default', tableName];

    const response = await unary(
      this.collectionStub,
      'GetCollection',
      {
        namespace,
        name: collectionName,
      },
      this.metadata
    );

    if (response.error) {
      throw new Error(response.error);
    }

    return response.collection;
  }

  async executeQuery(query) {
    const parsed = parseSelectCollectionQuery(query);
    if (!parsed) {
      throw new Error('Remote query support is currently limited to SHOW/SELECT collections and namespaces.');
    }

    if (parsed.kind === 'namespaces') {
      const namespaces = await this.listNamespaces();
      const rows = namespaces.map((namespace) => [
        namespace.name,
        Number(namespace.stats?.storage_bytes || 0),
        Number(namespace.stats?.vector_count || 0),
        Number(namespace.stats?.collection_count || 0),
      ]);
      return {
        columns: ['namespace', 'storage_bytes', 'vector_count', 'collection_count'],
        rows,
        stats: {
          row_count: rows.length,
          execution_time_ms: 0,
          scanned_rows: rows.length,
        },
      };
    }

    const namespaces = await this.listNamespaces().catch(() => []);
    const namespaceNames = namespaces.length > 0
      ? namespaces.map((ns) => ns.name).filter(Boolean)
      : ['default'];

    const collectionsByNamespace = await Promise.all(
      namespaceNames.map(async (namespace) => {
        const collections = await this.listCollections(namespace).catch(() => []);
        return collections.map((collection) => [
          namespace,
          collection.name,
          Number(collection.dimension || 0),
          String(collection.metric || ''),
          Number(collection.document_count || 0),
        ]);
      })
    );

    const rows = collectionsByNamespace.flat();
    return {
      columns: ['namespace', 'name', 'dimension', 'metric', 'document_count'],
      rows,
      stats: {
        row_count: rows.length,
        execution_time_ms: 0,
        scanned_rows: rows.length,
      },
    };
  }

  async callTool(toolName, args = {}) {
    if (toolName === 'sochdb_list_tables') {
      const tables = await this.listTables();
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(tables),
          },
        ],
      };
    }

    if (toolName === 'sochdb_describe') {
      const tableName = args.table || args.name;
      if (!tableName) {
        throw new Error('Missing table name');
      }
      const collection = await this.describeTable(String(tableName));
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(collection, null, 2),
          },
        ],
      };
    }

    if (toolName === 'sochdb_query') {
      const query = args.query;
      if (!query) {
        throw new Error('Missing query');
      }
      const result = await this.executeQuery(String(query));
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(
              result.rows.map((row) =>
                Object.fromEntries(result.columns.map((column, index) => [column, row[index]]))
              ),
              null,
              2
            ),
          },
        ],
      };
    }

    throw new Error(`Remote instance does not support MCP tool "${toolName}" yet.`);
  }

  getDiagnostics() {
    return {
      connected: Boolean(this.connectedAt),
      mode: 'remote',
      address: this.address,
      tls: this.tls,
      hasApiKey: Boolean(this.apiKey),
      serverInfo: this.serverInfo,
    };
  }
}
