import tailwindcss from "@tailwindcss/vite"
import { defineConfig } from "wxt"

// See https://wxt.dev/api/config.html
export default defineConfig({
  modules: ["@wxt-dev/module-react"],
  vite: () => ({
    plugins: [tailwindcss()],
  }),
  suppressWarnings: {
    firefoxDataCollection: true,
  },
  manifest: {
    name: "__MSG_extName__",
    description: "__MSG_extDescription__",
    default_locale: "en",
    permissions: ["tabs", "storage", "alarms", "notifications"],
    host_permissions: ["http://127.0.0.1:8000/*"],
    web_accessible_resources: [
      {
        matches: ["<all_urls>"],
        resources: ["/extensions-blocked.html"],
      },
    ],
  },
})
