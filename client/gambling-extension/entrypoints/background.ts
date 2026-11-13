import { bypassStorage } from "@/utils/storage"

const API_BASE = import.meta.env.WXT_API_BASE

interface ClassifyMsg {
  type: "classify"
  url: string
}

interface GamblingAlertMsg {
  type: "gambling_alert"
  url: string
  gambling_score: string
}

interface RedirectMsg {
  type: "redirect"
  url: string
  gambling_score: string
  from_list: string
}

type BgMsg = ClassifyMsg | GamblingAlertMsg | RedirectMsg

function isClassifyMsg(msg: BgMsg): msg is ClassifyMsg {
  return msg.type === "classify"
}

function isGamblingAlertMsg(msg: BgMsg): msg is GamblingAlertMsg {
  return msg.type === "gambling_alert"
}

function isRedirectMsg(msg: BgMsg): msg is RedirectMsg {
  return msg.type === "redirect"
}

function isExtensionsUrl(url: string): boolean {
  return (
    url.startsWith("chrome://extensions") ||
    url.startsWith("edge://extensions") ||
    url.startsWith("brave://extensions") ||
    url.startsWith("opera://extensions") ||
    url.startsWith("vivaldi://extensions") ||
    url.startsWith("about:addons")
  )
}

function getInstalledAt(): Promise<string | null> {
  return browser.storage.local
    .get("installed_at")
    .then((r) => (r.installed_at as string | undefined) ?? null)
}

function hasPassword(): Promise<boolean> {
  return browser.storage.local
    .get("partner_password")
    .then((r) => !!r.partner_password)
}

async function shouldBlockExtensions(): Promise<"block" | "warn" | "grace"> {
  const partner = await hasPassword()
  if (partner) return "block"
  const installedAt = await getInstalledAt()
  if (!installedAt) return "grace"
  const days =
    (Date.now() - new Date(installedAt).getTime()) / (1000 * 60 * 60 * 24)
  if (days >= 7) return "warn"
  return "grace"
}

async function handleExtensionsAccess(tabId: number) {
  const action = await shouldBlockExtensions()

  const bypassed = (await bypassStorage.getSession([
    "extensions_bypass",
    "extensions_bypass_expires_at",
  ])) as { extensions_bypass?: boolean; extensions_bypass_expires_at?: number }
  if (
    bypassed.extensions_bypass &&
    bypassed.extensions_bypass_expires_at! > Date.now()
  ) {
    return
  }
  if (bypassed.extensions_bypass) {
    await bypassStorage.clearSession([
      "extensions_bypass",
      "extensions_bypass_expires_at",
    ])
  }

  if (action === "block") {
    const blockedUrl = browser.runtime.getURL("/extensions-blocked.html")
    await browser.tabs.create({ url: blockedUrl, index: 0 })
    await browser.tabs.remove(tabId)
    return
  }

  if (action === "warn") {
    try {
      await browser.notifications.create({
        type: "basic",
        iconUrl: "/icon/128.png",
        title: "Gambling Blocker",
        message: "Security setup not complete. Protection may be compromised.",
      })
    } catch {}
    return
  }

  const installedAt = await getInstalledAt()
  if (!installedAt) return
  const days =
    (Date.now() - new Date(installedAt).getTime()) / (1000 * 60 * 60 * 24)
  const remaining = Math.ceil(7 - days)
  if (remaining > 0 && remaining <= 7) {
    try {
      await browser.notifications.create({
        type: "basic",
        iconUrl: "/icon/128.png",
        title: "Gambling Blocker",
        message: `Complete security setup within ${remaining} day(s) to enable full protection.`,
      })
    } catch {}
  }
}

export default defineBackground(() => {
  browser.runtime.onMessage.addListener(
    (msg: BgMsg, sender: Browser.runtime.MessageSender) => {
      if (isClassifyMsg(msg)) {
        return fetch(
          `${API_BASE}/classify/url-fused?url=${encodeURIComponent(msg.url)}`
        ).then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`)
          return res.json()
        })
      }

      if (isGamblingAlertMsg(msg)) {
        hasPassword().then((hasPartner) => {
          if (!hasPartner) return
          getExtensionId().then((extId) => {
            if (!extId) return
            fetch(`${API_BASE}/extension/gambling-alert`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                extension_id: extId,
                url: msg.url,
                gambling_score: parseFloat(msg.gambling_score),
              }),
            }).catch(() => {})
          })
        })
        return
      }

      if (isRedirectMsg(msg)) {
        const tabId = sender.tab?.id
        if (!tabId) return
        const qp = new URLSearchParams({
          url: msg.url,
          gambling_score: msg.gambling_score,
          from_list: msg.from_list,
        })
        const blockedUrl = browser.runtime.getURL("/blocked.html") + "?" + qp
        browser.tabs.update(tabId, { url: blockedUrl })
      }
    }
  )

  browser.tabs.onUpdated.addListener((tabId, changeInfo) => {
    const url = changeInfo.url
    if (url && isExtensionsUrl(url)) {
      handleExtensionsAccess(tabId)
    }
  })

  browser.tabs.onActivated.addListener(async (activeInfo) => {
    const tab = await browser.tabs.get(activeInfo.tabId)
    if (tab.url && isExtensionsUrl(tab.url)) {
      handleExtensionsAccess(tab.id!)
    }
  })

  browser.alarms.onAlarm.addListener(async (alarm) => {
    if (alarm.name === "heartbeat") {
      const hasPartner = await hasPassword()
      if (!hasPartner) return
      const extId = await getExtensionId()
      if (!extId) return
      fetch(`${API_BASE}/extension/heartbeat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ extension_id: extId }),
      }).catch(() => {})
    }
  })

  browser.runtime.onInstalled.addListener(async () => {
    const installed = await getInstalledAt()
    if (!installed) {
      await browser.storage.local.set({
        installed_at: new Date().toISOString(),
      })
    }
    try {
      await browser.alarms.create("heartbeat", { periodInMinutes: 30 })
    } catch {}
    const hasPartner = await hasPassword()
    if (!hasPartner) return
    const extId = await getExtensionId()
    if (extId) {
      fetch(`${API_BASE}/extension/heartbeat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ extension_id: extId }),
      }).catch(() => {})
    }
  })
})

async function getExtensionId(): Promise<string | null> {
  try {
    const data = await browser.storage.local.get("extension_id")
    if (data.extension_id) return data.extension_id as string
    const id = crypto.randomUUID()
    await browser.storage.local.set({ extension_id: id })
    return id
  } catch {
    return null
  }
}
