/// <reference types="vite/client" />

// Vite asset imports: ?url returns the resolved URL to a static asset.
declare module "*?url" {
  const url: string;
  export default url;
}

// Vite WASM imports.
declare module "*.wasm?url" {
  const url: string;
  export default url;
}
