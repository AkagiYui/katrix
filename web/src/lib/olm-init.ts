// Olm (libolm) WASM initialisation. The WASM binary is bundled by Vite and
// loaded once on first use. All E2EE operations depend on `initOlm()` having
// completed.
import Olm from "@matrix-org/olm";
// Vite ?url import gives the resolved URL to olm.wasm at build/dev time.
import olmWasmUrl from "@matrix-org/olm/olm.wasm?url";

let initPromise: Promise<void> | null = null;

/** Initialise the Olm WASM module. Safe to call repeatedly; only runs once. */
export function initOlm(): Promise<void> {
  if (initPromise) return initPromise;
  initPromise = (async () => {
    await Olm.init({ locateFile: () => olmWasmUrl });
  })().catch((e) => {
    // Reset so a later retry can proceed.
    initPromise = null;
    throw e;
  });
  return initPromise;
}

export default Olm;
