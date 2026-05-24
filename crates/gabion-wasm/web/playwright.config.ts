import { defineConfig, devices } from '@playwright/test';

// A minimal harness for self-evaluating layout and visual quality (and for the
// in-browser regression check the R2 note flagged: confirm the real wasm core
// boots and gossips in a browser). It serves the production build via
// `vite preview`, so run `pnpm run build` first.
export default defineConfig({
  testDir: './tests',
  outputDir: './test-results',
  fullyParallel: false,
  use: {
    baseURL: 'http://localhost:4173',
    viewport: { width: 1440, height: 900 },
    deviceScaleFactor: 2,
  },
  webServer: {
    command: 'pnpm run preview -- --port 4173 --strictPort',
    url: 'http://localhost:4173',
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
});
