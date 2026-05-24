import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

// The wasm package is produced by `wasm-pack --target web` (see package.json
// `build:wasm`). That target's glue loads the module via
// `new URL('gabion_wasm_bg.wasm', import.meta.url)`, which Vite resolves and
// emits natively — so no `vite-plugin-wasm` is needed. See web/README.md for
// why this deviates from the plan's stated plugin.
export default defineConfig({
  plugins: [svelte()],
  build: {
    target: 'esnext',
  },
});
