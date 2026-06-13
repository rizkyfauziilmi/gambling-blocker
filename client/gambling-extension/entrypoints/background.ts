const API_BASE = import.meta.env.WXT_API_BASE

interface ClassifyMsg {
  type: "classify"
  url: string
}

interface RedirectMsg {
  type: "redirect"
  url: string
  gambling_score: string
  from_list: string
}

type BgMsg = ClassifyMsg | RedirectMsg

function isClassifyMsg(msg: BgMsg): msg is ClassifyMsg {
  return msg.type === "classify"
}

function isRedirectMsg(msg: BgMsg): msg is RedirectMsg {
  return msg.type === "redirect"
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
})
