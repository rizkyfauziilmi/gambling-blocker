const SESSION_KEYS = ["extensions_bypass", "extensions_bypass_expires_at"]

function getSessionStore() {
  if (typeof browser.storage?.session !== "undefined") {
    return browser.storage.session
  }
  return browser.storage.local
}

export const bypassStorage = {
  async getSession(keys?: string | string[]) {
    const store = getSessionStore() as { get: (keys: string | string[] | null) => Promise<Record<string, unknown>> }
    return store.get(keys ?? SESSION_KEYS)
  },
  async setSession(data: Record<string, unknown>) {
    const store = getSessionStore() as { set: (data: Record<string, unknown>) => Promise<void> }
    await store.set(data)
  },
  async clearSession(keys?: string | string[]) {
    const store = getSessionStore() as { remove: (keys: string | string[]) => Promise<void> }
    await store.remove(keys ?? SESSION_KEYS)
  },
}
