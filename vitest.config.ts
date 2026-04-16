import { defineWorkersConfig } from "@cloudflare/vitest-pool-workers/config";

export default defineWorkersConfig({
  test: {
    poolOptions: {
      workers: {
        wrangler: { configPath: "./wrangler.jsonc" },
        // Isolated storage must be disabled because the ProxyDO uses setAlarm()
        // during publish. Alarms fire asynchronously after the test completes,
        // which prevents the isolated storage frame from being popped cleanly.
        // See: https://developers.cloudflare.com/workers/testing/vitest-integration/known-issues/#isolated-storage
        isolatedStorage: false,
      },
    },
  },
});
