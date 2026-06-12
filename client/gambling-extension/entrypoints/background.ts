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
  browser.tabs.onUpdated.addListener(async (tabId, changeInfo, tab) => {
    console.log("[BG] onUpdated:", {
      tabId,
      status: changeInfo.status,
      url: tab.url,
    })

    if (changeInfo.status !== "loading") return
    if (!tab.url || !tab.url.startsWith("http")) return
    if (shouldSkip(tab.url)) return

    // Compare by hostname (not full URL) to survive URL changes like
    // Google adding &sei=... on redirect.
    const tabHostname = new URL(tab.url).hostname
    const storage = browser.storage.session || browser.storage.local
    const { recentlyChecked } = (await storage.get(
      "recentlyChecked",
    )) as { recentlyChecked?: { hostname: string; ts: number } }
    if (
      recentlyChecked?.hostname === tabHostname &&
      Date.now() - recentlyChecked.ts < 30000
    ) {
      console.log("[BG] skip recently checked:", tab.url)
      // Don't remove flag — let it expire via 30s TTL check to prevent
      // loops from Chrome firing multiple onUpdated events.
      return
    }

    console.log("[BG] redirecting to loading page:", tab.url)
    // Set flag before redirect so loading page can redirect back without looping
    storage.set({
      recentlyChecked: { hostname: tabHostname, ts: Date.now() },
    })
    const params = new URLSearchParams({ url: tab.url })
    const loadingUrl =
      browser.runtime.getURL("/loading.html" as never) + "?" + params
    browser.tabs.update(tabId, { url: loadingUrl })
  })
})
