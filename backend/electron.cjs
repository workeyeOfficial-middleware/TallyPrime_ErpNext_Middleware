const { app, BrowserWindow, shell } = require("electron");
const path    = require("path");
const { spawn } = require("child_process");
const http    = require("http");

let mainWindow;
let backendProcess;

const PORT = 4000;

// ── Resolve backend EXE path ──────────────────────────────────────────────────
// In production (installed): resources/backend/tally-middleware-backend.exe
// In dev: ../dist/tally-middleware-backend.exe
function getBackendPath() {
  if (app.isPackaged) {
    return path.join(process.resourcesPath, "backend", "tally-middleware-backend.exe");
  }
  return path.join(__dirname, "dist", "tally-middleware-backend.exe");
}

// ── Start backend EXE ─────────────────────────────────────────────────────────
function startBackend() {
  const exePath = getBackendPath();
  console.log("Starting backend from:", exePath);

  backendProcess = spawn(exePath, [], {
    detached: false,
    stdio:    "ignore",
    windowsHide: true,
    cwd: path.dirname(exePath),
  });

  backendProcess.on("error", (err) => {
    console.error("Backend failed to start:", err.message);
  });

  backendProcess.on("exit", (code) => {
    console.log("Backend exited with code:", code);
  });
}

// ── Wait for backend to be ready ──────────────────────────────────────────────
function waitForBackend(retries = 30, delay = 1000) {
  return new Promise((resolve, reject) => {
    let attempts = 0;
    const check = () => {
      http.get(`http://localhost:${PORT}`, () => {
        resolve();
      }).on("error", () => {
        attempts++;
        if (attempts >= retries) {
          reject(new Error("Backend did not start in time"));
        } else {
          setTimeout(check, delay);
        }
      });
    };
    check();
  });
}

// ── Create app window ─────────────────────────────────────────────────────────
function createWindow() {
  mainWindow = new BrowserWindow({
    width:  1280,
    height: 800,
    title:  "Tally ERPNext Integration",
    icon:   path.join(__dirname, "assets", "icon.png"),
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
    },
  });

  mainWindow.loadURL(`http://localhost:${PORT}`);
  mainWindow.setMenuBarVisibility(false);

  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: "deny" };
  });

  mainWindow.on("closed", () => {
    mainWindow = null;
  });
}

// ── App lifecycle ─────────────────────────────────────────────────────────────
app.whenReady().then(async () => {
  // Kill any existing backend on port 4000 before starting fresh
  try {
    await new Promise((resolve) => {
      http.get(`http://localhost:${PORT}`, () => {
        // Already running — don't start a new one
        console.log("Backend already running on port 4000");
        resolve();
      }).on("error", () => {
        // Not running — start it
        startBackend();
        resolve();
      });
    });
  } catch (_) {
    startBackend();
  }

  try {
    await waitForBackend();
    createWindow();
  } catch (err) {
    console.error(err.message);
    app.quit();
  }
});

app.on("window-all-closed", () => {
  if (backendProcess) {
    try { backendProcess.kill(); } catch (_) {}
  }
  app.quit();
});

app.on("activate", () => {
  if (mainWindow === null) createWindow();
});
