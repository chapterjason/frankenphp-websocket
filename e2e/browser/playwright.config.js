const { defineConfig, devices } = require('@playwright/test');

module.exports = defineConfig({
  testDir: '.',
  reporter: 'list',
  use: {
    baseURL: process.env.WEBSOCKET_ORIGIN,
    // The stack uses Caddy's internal CA, which the browsers do not trust.
    ignoreHTTPSErrors: true,
  },
  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
    { name: 'firefox', use: { ...devices['Desktop Firefox'] } },
  ],
});
