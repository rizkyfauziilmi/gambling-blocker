const API_BASE = import.meta.env.WXT_API_BASE

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
  const recentlyChecked = new Map<number, { hostname: string; ts: number }>()

  browser.tabs.onUpdated.addListener(async (tabId, changeInfo, tab) => {
    console.log("[BG] onUpdated:", {
      tabId,
      status: changeInfo.status,
      url: tab.url,
    })

    if (changeInfo.status !== "loading") return
    if (!tab.url || !tab.url.startsWith("http")) return
    if (shouldSkip(tab.url)) return

    const tabHostname = new URL(tab.url).hostname
    const prev = recentlyChecked.get(tabId)
    const now = Date.now()

    // Skip jika hostname yg sama di tab yg sama dalam 2 detik.
    // Mencegah: (1) redirect-back dari loading page, (2) duplicate Chrome event.
    if (prev?.hostname === tabHostname && now - prev.ts < 2000) {
      console.log("[BG] skip recently checked:", tab.url)
      return
    }

    console.log("[BG] redirecting to loading page:", tab.url)
    recentlyChecked.set(tabId, { hostname: tabHostname, ts: now })
    const params = new URLSearchParams({ url: tab.url })
    const loadingUrl =
      browser.runtime.getURL("/loading.html" as never) + "?" + params
    browser.tabs.update(tabId, { url: loadingUrl })
  })
})
