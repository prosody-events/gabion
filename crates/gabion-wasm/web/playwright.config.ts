import { defineConfig } from '@playwright/test';

// Suites against the production build, served by `vite preview` (run
// `pnpm run build` first). `screenshot.spec.ts` is the functional smoke, run
// once at 1440 px in the light theme, and also confirms the real wasm core
// boots and gossips in a browser. `audit.spec.ts` is the automated design audit
// (README "Design rules") and `story.spec.ts` walks each scenario's story by
// its next actions, both at five widths in each theme with motion reduced.
// `motion.spec.ts` checks each packet's line and its counts, cause before
// effect, once at 1440 px in the light theme with motion on; the story walks
// run there too. `next.spec.ts` checks the next action, the goals and the
// heading as pure functions, with no page.
const audits = [1440, 1280, 1100, 768, 400].flatMap((width) =>
  (['light', 'dark'] as const).map((colorScheme) => ({
    name: `audit-${width}-${colorScheme}`,
    testMatch: /(audit|story)\.spec\.ts$/,
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
    // The next action, the goals and the heading as pure functions: no page.
    { name: 'next', testMatch: /next\.spec\.ts$/ },
    {
      name: 'screenshot',
      testMatch: /screenshot\.spec\.ts$/,
      use: { viewport: { width: 1440, height: 900 } },
    },
    ...audits,
    {
      name: 'audit-1440-light-motion',
      testMatch: /(motion|story)\.spec\.ts$/,
      use: { viewport: { width: 1440, height: 900 }, colorScheme: 'light', reducedMotion: 'no-preference' },
    },
  ],
});
