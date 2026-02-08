import { DeltaT } from "../src/index";
import { spawn, type Subprocess } from "bun";
import * as net from "node:net";
import * as fs from "node:fs";
import * as path from "node:path";

let serverProcess: Subprocess | null = null;
let serverPort: number = 0;
let dataDir: string = "";

const PROJECT_ROOT = path.resolve(import.meta.dir, "../..");

function findFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (addr && typeof addr === "object") {
        const port = addr.port;
        server.close(() => resolve(port));
      } else {
        reject(new Error("failed to get port"));
      }
    });
    server.on("error", reject);
  });
}

async function waitForTcp(port: number, timeout = 30000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    try {
      await new Promise<void>((resolve, reject) => {
        const sock = net.createConnection({ port, host: "127.0.0.1" }, () => {
          sock.destroy();
          resolve();
        });
        sock.on("error", reject);
      });
      return;
    } catch {
      await new Promise((r) => setTimeout(r, 100));
    }
  }
  throw new Error(`server did not start within ${timeout}ms`);
}

export async function startServer(): Promise<number> {
  serverPort = await findFreePort();
  dataDir = `/tmp/deltat_client_test_${Date.now()}`;
  fs.mkdirSync(dataDir, { recursive: true });

  const binary = path.join(PROJECT_ROOT, "target/release/deltat");
  if (!fs.existsSync(binary)) {
    throw new Error(`Binary not found at ${binary}. Run 'cargo build --release' first.`);
  }

  serverProcess = spawn({
    cmd: [binary],
    env: {
      ...process.env,
      DELTAT_PORT: String(serverPort),
      DELTAT_BIND: "127.0.0.1",
      DELTAT_DATA_DIR: dataDir,
      DELTAT_PASSWORD: "deltat",
    },
    stdout: "ignore",
    stderr: "ignore",
  });

  await new Promise((r) => setTimeout(r, 200));
  if (serverProcess.exitCode !== null) {
    throw new Error(`Server exited immediately with code ${serverProcess.exitCode}`);
  }

  await waitForTcp(serverPort);
  return serverPort;
}

export function stopServer(): void {
  if (serverProcess) {
    serverProcess.kill();
    serverProcess = null;
  }
  try {
    fs.rmSync(dataDir, { recursive: true, force: true });
  } catch {
    // ignore cleanup errors
  }
}

export function createClient(port?: number, database?: string): DeltaT {
  return new DeltaT({
    host: "127.0.0.1",
    port: port ?? serverPort,
    database: database ?? "client_test",
    password: "deltat",
    username: "deltat",
  });
}
