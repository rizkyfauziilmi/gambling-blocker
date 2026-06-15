const STORAGE_KEY = "partner_password"
const EXTENSION_ID_KEY = "extension_id"
const INSTALLED_AT_KEY = "installed_at"

async function getPasswordData(): Promise<{
  hash: string
  salt: string
} | null> {
  try {
    const data = (await browser.storage.local.get(STORAGE_KEY)) as {
      [STORAGE_KEY]?: { hash: string; salt: string }
    }
    return data[STORAGE_KEY] || null
  } catch {
    return null
  }
}

async function setPasswordData(hash: string, salt: string): Promise<void> {
  await browser.storage.local.set({ [STORAGE_KEY]: { hash, salt } })
}

export async function hasPassword(): Promise<boolean> {
  const data = await getPasswordData()
  return data !== null
}

export async function verifyPassword(password: string): Promise<boolean> {
  const data = await getPasswordData()
  if (!data) return false
  const hash = await pbkdf2(password, data.salt)
  return hash === data.hash
}

export async function getOrCreateExtensionId(): Promise<string> {
  try {
    const data = (await browser.storage.local.get(EXTENSION_ID_KEY)) as {
      [EXTENSION_ID_KEY]?: string
    }
    if (data[EXTENSION_ID_KEY]) return data[EXTENSION_ID_KEY]
  } catch {}
  const id = crypto.randomUUID()
  await browser.storage.local.set({ [EXTENSION_ID_KEY]: id })
  return id
}

export async function getInstalledAt(): Promise<string | null> {
  try {
    const data = (await browser.storage.local.get(INSTALLED_AT_KEY)) as {
      [INSTALLED_AT_KEY]?: string
    }
    return data[INSTALLED_AT_KEY] || null
  } catch {
    return null
  }
}

export async function getPartnerStatus(): Promise<{
  hasPartner: boolean
  daysSinceInstall: number
}> {
  const hasPartner = await hasPassword()
  const installedAt = await getInstalledAt()
  let daysSinceInstall = 0
  if (installedAt) {
    const diff = Date.now() - new Date(installedAt).getTime()
    daysSinceInstall = Math.floor(diff / (1000 * 60 * 60 * 24))
  }
  return { hasPartner, daysSinceInstall }
}

async function pbkdf2(
  password: string,
  salt: string,
  iterations = 600000
): Promise<string> {
  const enc = new TextEncoder()
  const keyMaterial = await crypto.subtle.importKey(
    "raw",
    enc.encode(password),
    "PBKDF2",
    false,
    ["deriveBits"]
  )
  const bits = await crypto.subtle.deriveBits(
    {
      name: "PBKDF2",
      salt: enc.encode(salt),
      iterations,
      hash: "SHA-256",
    },
    keyMaterial,
    256
  )
  return Array.from(new Uint8Array(bits))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("")
}
