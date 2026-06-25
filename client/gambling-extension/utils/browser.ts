export function getExtensionsUrl(): string {
  if (import.meta.env.FIREFOX) return "about:addons"
  return "chrome://extensions"
}
