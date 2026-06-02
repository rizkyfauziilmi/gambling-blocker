const API_BASE = import.meta.env.WXT_API_BASE
const API_CLASSIFY_URL = import.meta.env.WXT_API_CLASSIFY_URL

function shouldSkip(url: string): boolean {
  try {
    const parsed = new URL(url)
    if (
      parsed.protocol === "chrome-extension:" ||
      parsed.protocol === "moz-extension:"
    )
      return true
    if (
      parsed.hostname === "127.0.0.1" ||
      parsed.hostname === "localhost" ||
      parsed.hostname === "[::1]" ||
      parsed.hostname === "0.0.0.0"
    )
      return true
    return false
  } catch {
    return true
  }
}

export default defineBackground(() => {
  browser.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
    console.log("[BG] onUpdated:", {
      tabId,
      status: changeInfo.status,
      url: tab.url,
    })

    if (changeInfo.status !== "loading") return
    if (!tab.url || !tab.url.startsWith("http")) return
    if (shouldSkip(tab.url)) return

    const params = new URLSearchParams({ url: tab.url })
    const fullUrl = `${API_BASE}/classify/url?${params}`
    console.log("[BG] fetching:", fullUrl)

    fetch(fullUrl)
      .then((res) => res.json())
      .then((data) => {
        console.log("[BG] response:", data)
        if (data.category === "gambling") {
          console.log("[BG] blocking! redirecting to blocked.html")
          const qp = new URLSearchParams({
            url: tab.url!,
            gambling_score: String(data.gambling_score),
            from_list: data.from_list || "",
          })
          const blocked = browser.runtime.getURL("/blocked.html") + "?" + qp
          browser.tabs.update(tabId, { url: blocked })
        }
      })
      .catch((err) => console.error("[BG] fetch error:", err))
  })
})
