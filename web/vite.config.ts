import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Vite config: build output goes to internal/webui/dist so the Go binary can
// embed it via go:embed. The dev server proxies Matrix API prefixes to the
// local Go server.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../internal/webui/dist",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/_matrix": "http://localhost:8008",
      "/_synapse": "http://localhost:8008",
      "/.well-known": "http://localhost:8008",
    },
  },
});
