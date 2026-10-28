import { defineConfig } from '@playwright/test';

// Two suites against the production build, served by `vite preview` (run
// `pnpm run build` first). `screenshot.spec.ts` is the functional smoke, run
// once at 1440 px in the light theme, and also confirms the real wasm core
// boots and gossips in a browser. `audit.spec.ts` is the automated design audit
// (README "Design rules"), run at five widths in each theme with motion reduced.
const audits = [1440, 1280, 1100, 768, 400].flatMap((width) =>
  (['light', 'dark'] as const).map((colorScheme) => ({
    name: `audit-${width}-${colorScheme}`,
    testMatch: /audit\.spec\.ts$/,
    use: { viewport: { width, height: 900 }, colorScheme, reducedMotion: 'reduce' as const },
  })),
);

export default defineConfig({
  testDir: './tests',
  outputDir: './test-results',
  fullyParallel: false,
  use: { baseURL: 'http://localhost:4173' },
  webServer: {
    command: 'pnpm run preview -- --port 4173 --strictPort',
    url: 'http://localhost:4173',
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
  projects: [
    {
      name: 'chromium',
      testMatch: /screenshot\.spec\.ts$/,
      use: { viewport: { width: 1440, height: 900 } },
    },
    ...audits,
  ],
});
