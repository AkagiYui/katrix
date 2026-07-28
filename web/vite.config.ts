import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Vite config: build output goes to internal/webui/dist so the Go binary can
// embed it via go:embed. The dev server proxies Matrix API prefixes to the
// local Go server. The @matrix-org/olm WASM binary is excluded from Vite's
// dependency optimisation so the ?url import resolves to the raw asset.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../internal/webui/dist",
    emptyOutDir: true,
    // olm.wasm must be emitted as a static asset (not inlined) so the runtime
    // can fetch it. Assets over this size are emitted as separate files.
    assetsInlineLimit: 4096,
  },
  optimizeDeps: {
    exclude: ["@matrix-org/olm"],
  },
  server: {
    proxy: {
      "/_matrix": "http://localhost:8008",
      "/_synapse": "http://localhost:8008",
      "/.well-known": "http://localhost:8008",
    },
  },
});
