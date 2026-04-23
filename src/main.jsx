import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App.jsx";

import "./index.css";
import "./ui/api.js";
import "./lib/auth.js";
import "./lib/voices.js";

if (typeof window !== "undefined") {
  import("./lib/sessionHeartbeat.js").catch((e) => {
    console.warn("sessionHeartbeat init skipped:", e);
  });
}


const ROOT_ID = "root";

function mountApp() {
  const container = document.getElementById(ROOT_ID);

  if (!container) {
    console.error("[BOOT] root container not found");
    return;
  }

  if (!container.__orkioReactRoot) {
    container.__orkioReactRoot = ReactDOM.createRoot(container);
  }

  container.__orkioReactRoot.render(
    <React.StrictMode>
      <App />
    </React.StrictMode>
  );
}

mountApp();
