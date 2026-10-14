import { existsSync, readFileSync } from "node:fs";
import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";

// The development server serves over HTTPS with the same self-signed certificate
// that `task dev:certs` writes for the controller. The session cookie carries the
// __Host- prefix, which a browser stores only over HTTPS, so a plain HTTP dev
// server could never hold a session. The build itself never reads these files.
const key = "../.dev/api.key";
const certificate = "../.dev/api.crt";
const https = existsSync(key) && existsSync(certificate) ? { key: readFileSync(key), cert: readFileSync(certificate) } : undefined;

// The controller serves these files from one origin and the application routes in the
// browser, so asset paths must be absolute. A relative path resolves against the current
// route, and a deep link would then request assets that do not exist.
export default defineConfig({
  // A component test renders the real component, so the plugin must compile for the
  // browser condition even under the test runner.
  plugins: [svelte()],
  resolve: { conditions: ["browser"] },
  base: "/",
  build: { outDir: "../internal/web/dist", emptyOutDir: true, target: "es2022", sourcemap: false },
  server: {
    https,
    // The browser sees one origin, so it sends Sec-Fetch-Site: same-origin and the
    // API accepts the request. `secure: false` trusts the controller's self-signed
    // certificate, which exists only on a workstation.
    proxy: { "/v1": { target: "https://127.0.0.1:8443", secure: false } },
  },
  // The test runner stubs styles except the global sheet. Times read in UTC, so a
  // test that names a day passes in every time zone.
  test: { environment: "jsdom", include: ["src/**/*.test.ts"], globals: true, env: { TZ: "UTC" }, css: { include: [/app\.css/] } },
});
