export function hostnameFromUrl(url: string): string {
  const parsedUrl = new URL(url)
  return parsedUrl.hostname
}
