import { initLanguage, t } from "@/utils/i18n"

function shouldSkip(url: string): boolean {
  try {
    const p = new URL(url)
    if (p.protocol === "chrome-extension:" || p.protocol === "moz-extension:")
      return true
    if (
      p.hostname === "127.0.0.1" ||
      p.hostname === "localhost" ||
      p.hostname === "[::1]" ||
      p.hostname === "0.0.0.0"
    )
      return true
    return false
  } catch {
    return true
  }
}

export default defineContentScript({
  matches: ["*://*/*"],
  runAt: "document_start",
  async main(ctx) {
    await initLanguage()

    const url = window.location.href
    if (shouldSkip(url)) return

    const cleanUrl = url.split("#")[0]

    // --- inject overlay styles ---
    const style = document.createElement("style")
    style.textContent = `
      #gb-overlay {
        all: initial;
        position: fixed !important;
        inset: 0 !important;
        z-index: 2147483647 !important;
        background: #ffffff !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        font-family: system-ui, -apple-system, sans-serif !important;
      }
      #gb-overlay .gb-container {
        text-align: center !important;
      }
      #gb-overlay .gb-spinner {
        width: 48px;
        height: 48px;
        margin: 0 auto 16px;
        border: 4px solid #e5e7eb;
        border-top-color: #6366f1;
        border-radius: 50%;
        animation: gb-spin 0.8s linear infinite;
      }
      @keyframes gb-spin {
        to { transform: rotate(360deg); }
      }
      #gb-overlay h1 {
        font-size: 18px !important;
        font-weight: 600 !important;
        color: #111827 !important;
        margin: 0 0 8px !important;
        line-height: 1.5 !important;
      }
      #gb-overlay p {
        font-size: 14px !important;
        color: #6b7280 !important;
        margin: 0 !important;
        line-height: 1.5 !important;
      }
    `
    document.documentElement.appendChild(style)

    // --- inject overlay div ---
    const overlay = document.createElement("div")
    overlay.id = "gb-overlay"
    overlay.innerHTML = `
      <div class="gb-container">
        <div class="gb-spinner"></div>
        <h1>${t("loading_title")}</h1>
        <p>${t("loading_description")}</p>
      </div>
    `
    document.documentElement.appendChild(overlay)

    // --- block user interaction ---
    const prevOverflow = document.documentElement.style.overflow
    document.documentElement.style.overflow = "hidden"

    const escHandler = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault()
        e.stopPropagation()
      }
    }
    document.addEventListener("keydown", escHandler, { capture: true })

    const blockScroll = (e: Event) => e.preventDefault()
    document.addEventListener("wheel", blockScroll, { passive: false })
    document.addEventListener("touchmove", blockScroll, { passive: false })

    // classify via background
    browser.runtime
      .sendMessage({ type: "classify", url: cleanUrl })
      .then((data: any) => {
        if (!ctx.isValid) return
        if (data.category === "gambling") {
          browser.runtime
            .sendMessage({
              type: "redirect",
              url: cleanUrl,
              gambling_score: String(data.gambling_score),
              from_list: data.from_list || "",
            })
            .catch(() => {
              const qp = new URLSearchParams({
                url: cleanUrl,
                gambling_score: String(data.gambling_score),
                from_list: data.from_list || "",
              })
              window.location.href =
                `chrome-extension://${browser.runtime.id}/blocked.html?` + qp
            })
        } else {
          cleanup()
        }
      })
      .catch(() => {
        if (!ctx.isValid) return
        cleanup()
      })

    function cleanup() {
      document.documentElement.style.overflow = prevOverflow
      document.removeEventListener("keydown", escHandler, { capture: true })
      document.removeEventListener("wheel", blockScroll)
      document.removeEventListener("touchmove", blockScroll)
      style.remove()
      overlay.remove()
    }
  },
})
