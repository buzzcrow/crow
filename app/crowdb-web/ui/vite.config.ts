/// <reference types="vitest" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// During `npm run dev`, Vite serves the SPA on port 5173 and proxies
// `/api/*` and `/healthz` to the local Axum backend on 14000 (the
// crowdb-web default). For production, `npm run build`
// emits to `dist/`, which Axum serves directly via ServeDir.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://127.0.0.1:14000",
      "/healthz": "http://127.0.0.1:14000",
      "/internal": "http://127.0.0.1:14000",
    },
  },
  preview: {
    proxy: {
      "/api": "http://127.0.0.1:14000",
      "/healthz": "http://127.0.0.1:14000",
      "/internal": "http://127.0.0.1:14000",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: false,
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test/setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
    css: false,
  },
});
