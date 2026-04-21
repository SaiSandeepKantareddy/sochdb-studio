import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function resolveMcpSpawn(dbPath) {
  const configuredBin = process.env.SOCHDB_MCP_BIN;
  if (configuredBin) {
    return {
      command: configuredBin,
      args: ['--db', dbPath],
    };
  }

  const manifestPath = path.resolve(__dirname, '../../sochdb/sochdb-mcp/Cargo.toml');
  return {
    command: 'cargo',
    args: ['run', '--quiet', '--manifest-path', manifestPath, '--', '--db', dbPath],
  };
}

export class McpClient {
  constructor(dbPath) {
    this.dbPath = dbPath;
    this.child = null;
    this.stderrBuffer = [];
    this.nextId = 1;
    this.pending = new Map();
    this.ready = false;
    this.serverInfo = null;
  }

  async start() {
    if (this.child) return;

    const { command, args } = resolveMcpSpawn(this.dbPath);
    this.child = spawn(command, args, {
      stdio: ['pipe', 'pipe', 'pipe'],
      cwd: path.resolve(__dirname, '..'),
      env: {
        ...process.env,
        RUST_LOG: process.env.RUST_LOG || 'error',
      },
    });

    this.child.on('exit', (code, signal) => {
      const message = `MCP process exited (${code ?? 'null'}${signal ? `, ${signal}` : ''})`;
      for (const { reject } of this.pending.values()) {
        reject(new Error(message));
      }
      this.pending.clear();
      this.child = null;
      this.ready = false;
    });

    const stdoutRl = createInterface({ input: this.child.stdout });
    stdoutRl.on('line', (line) => {
      if (!line.trim()) return;
      try {
        const message = JSON.parse(line);
        const pending = this.pending.get(message.id);
        if (pending) {
          this.pending.delete(message.id);
          if (message.error) {
            pending.reject(new Error(message.error.message || JSON.stringify(message.error)));
          } else {
            pending.resolve(message.result);
          }
        }
      } catch (error) {
        // Ignore malformed lines; stderr captures useful context.
      }
    });

    const stderrRl = createInterface({ input: this.child.stderr });
    stderrRl.on('line', (line) => {
      if (line.trim()) {
        this.stderrBuffer.push(line.trim());
        if (this.stderrBuffer.length > 50) {
          this.stderrBuffer.shift();
        }
      }
    });

    const initResult = await this.request('initialize', {
      protocolVersion: '2024-11-05',
      clientInfo: {
        name: 'sochdb-studio-web-backend',
        version: '0.1.0',
      },
    });

    this.serverInfo = initResult?.serverInfo ?? null;
    await this.notify('notifications/initialized', {});
    this.ready = true;
  }

  async stop() {
    if (!this.child) return;
    try {
      await this.request('shutdown', {});
    } catch {
      // Best-effort shutdown.
    }
    this.child.kill();
    this.child = null;
    this.ready = false;
  }

  async notify(method, params = {}) {
    if (!this.child) throw new Error('MCP client is not running');
    const payload = {
      jsonrpc: '2.0',
      method,
      params,
    };
    this.child.stdin.write(`${JSON.stringify(payload)}\n`);
  }

  async request(method, params = {}) {
    if (!this.child) {
      throw new Error('MCP client is not running');
    }

    const id = this.nextId++;
    const payload = {
      jsonrpc: '2.0',
      id,
      method,
      params,
    };

    const responsePromise = new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });

    this.child.stdin.write(`${JSON.stringify(payload)}\n`);
    return responsePromise;
  }

  async listTools() {
    return this.request('tools/list', {});
  }

  async callTool(name, args = {}) {
    return this.request('tools/call', {
      name,
      arguments: args,
    });
  }

  getDiagnostics() {
    return {
      ready: this.ready,
      dbPath: this.dbPath,
      serverInfo: this.serverInfo,
      stderr: [...this.stderrBuffer],
    };
  }
}
