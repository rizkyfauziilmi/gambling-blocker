import { useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

console.log(
  "[LOADING] script loaded, browser:",
  typeof browser !== "undefined" ? navigator.userAgent : "no browser API"
)

const API_BASE = import.meta.env.WXT_API_BASE
const t = (key: string, ...args: (string | number)[]) =>
  browser.i18n.getMessage(key as never, args.map(String))

function App() {
  const params = new URLSearchParams(location.search)
  const originalUrl = params.get("url") || ""
  const [text, setText] = useState(t("loading_description"))

  useEffect(() => {
    if (!originalUrl) {
      setText("No URL provided")
      return
    }

    const controller = new AbortController()

    fetch(
      `${API_BASE}/classify/url-fused?url=${encodeURIComponent(originalUrl)}`,
      { signal: controller.signal }
    )
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then(async (data) => {
        console.log("[LOADING] API response:", JSON.stringify(data))
        if (data.category === "gambling") {
          console.log("[LOADING] gambling → redirect to blocked.html")
          const qp = new URLSearchParams({
            url: originalUrl,
            gambling_score: String(data.gambling_score),
            from_list: data.from_list || "",
          })
          location.href = browser.runtime.getURL("/blocked.html") + "?" + qp
        } else {
          console.log("[LOADING] safe → redirect back to:", originalUrl)
          location.href = originalUrl
        }
      })
      .catch((err) => {
        console.log("[LOADING] fetch error:", err)
        if (err instanceof DOMException && err.name === "AbortError") {
          console.log("[LOADING] aborted by unmount, waiting for remount")
          return
        }
        location.href = originalUrl
      })

    return () => controller.abort()
  }, [originalUrl])

  return (
    <div className="flex min-h-screen items-center justify-center bg-linear-to-b from-white to-gray-50">
      <div className="text-center">
        <Loader2 className="mx-auto mb-4 size-12 animate-spin text-indigo-500" />
        <h1 className="text-lg font-semibold text-gray-900">
          {t("loading_title")}
        </h1>
        <p className="mt-2 text-sm text-gray-500">{text}</p>
        {originalUrl && (
          <p className="mt-4 max-w-sm truncate text-xs text-gray-400">
            {originalUrl}
          </p>
        )}
      </div>
    </div>
  )
}

export default App
