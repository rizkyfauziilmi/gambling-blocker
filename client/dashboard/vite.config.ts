import path from "path"
import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react"
import type { Connect, Plugin, ProxyOptions } from "vite"
import { defineConfig } from "vite"

function basicAuthPlugin(): Plugin {
  const username = process.env.DASHBOARD_USERNAME || "admin"
  const password = process.env.DASHBOARD_PASSWORD || "admin123"

  return {
    name: "basic-auth",
    configureServer(server) {
      const handler: Connect.NextHandleFunction = (req, res, next) => {
        const auth = req.headers.authorization
        if (auth && auth.startsWith("Basic ")) {
          const decoded = Buffer.from(auth.slice(6), "base64").toString()
          const [user, pass] = decoded.split(":")
          if (user === username && pass === password) {
            next()
            return
          }
        }
        res.writeHead(401, { "WWW-Authenticate": 'Basic realm="Dashboard"' })
        res.end()
      }
      server.middlewares.use(handler)
    },
  }
}

function authProxy(): ProxyOptions {
  return {
    configure: (proxy) => {
      proxy.on("proxyReq", (proxyReq, req) => {
        if (req.headers.authorization) {
          proxyReq.setHeader("Authorization", req.headers.authorization)
        }
      })
    },
  }
}

export default defineConfig({
  plugins: [react(), tailwindcss(), basicAuthPlugin()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    proxy: {
      "/reports": { target: "http://127.0.0.1:8000", ...authProxy() },
      "/blacklist": { target: "http://127.0.0.1:8000", ...authProxy() },
      "/whitelist": { target: "http://127.0.0.1:8000", ...authProxy() },
      "/cache": { target: "http://127.0.0.1:8000", ...authProxy() },
      "/report": "http://127.0.0.1:8000",
      "/classify": "http://127.0.0.1:8000",
      "/settings": { target: "http://127.0.0.1:8000", ...authProxy() },
      "/logs": { target: "http://127.0.0.1:8000", ...authProxy() },
    },
  },
})
