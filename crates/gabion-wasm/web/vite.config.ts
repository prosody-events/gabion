import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

// The wasm package is produced by `wasm-pack --target web` (see package.json
// `build:wasm`). That target's glue loads the module via
// `new URL('gabion_wasm_bg.wasm', import.meta.url)`, which Vite resolves and
// emits natively — so no `vite-plugin-wasm` is needed. See web/README.md for
// why this deviates from the plan's stated plugin.
// The visualizer is deployed under GitHub Pages at
// `https://prosody-events.github.io/gabion/`, so the production build needs
// `base: '/gabion/'` — otherwise the built `index.html` references assets at
// `/assets/...` and 404s under the project-pages subpath. Local dev,
// `vite preview`, and the Playwright in-browser smoke all live at root, so
// the base defaults to `/`. CI flips it to `/gabion/` via `VITE_BASE` when
// building for Pages.
export default defineConfig({
  base: process.env.VITE_BASE ?? '/',
  plugins: [svelte()],
  build: {
    target: 'esnext',
  },
});
