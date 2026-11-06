import { defineConfig } from '@playwright/test';

// Three suites against the production build, served by `vite preview` (run
// `pnpm run build` first). `screenshot.spec.ts` is the functional smoke, run
// once at 1440 px in the light theme, and also confirms the real wasm core
// boots and gossips in a browser. `audit.spec.ts` is the automated design audit
// (README "Design rules"), run at five widths in each theme with motion reduced.
// `motion.spec.ts` checks the waves in motion, cause before effect, once at
// 1440 px in the light theme with motion on.
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
  // Never reuse a server already on the port: it may serve another build, and
  // the suites would pass against it.
  webServer: {
    command: 'pnpm exec vite preview --port 4173 --strictPort',
    url: 'http://localhost:4173',
    timeout: 120_000,
  },
  projects: [
    {
      name: 'screenshot',
      testMatch: /screenshot\.spec\.ts$/,
      use: { viewport: { width: 1440, height: 900 } },
    },
    ...audits,
    {
      name: 'audit-1440-light-motion',
      testMatch: /motion\.spec\.ts$/,
      use: { viewport: { width: 1440, height: 900 }, colorScheme: 'light', reducedMotion: 'no-preference' },
    },
  ],
});
