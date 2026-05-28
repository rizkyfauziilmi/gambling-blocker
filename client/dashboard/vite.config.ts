import path from "path"
import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vite"

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    proxy: {
      "/reports": "http://127.0.0.1:8000",
      "/report": "http://127.0.0.1:8000",
      "/classify": "http://127.0.0.1:8000",
    },
  },
})
