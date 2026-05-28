import { useEffect, useState } from "react";

const API_BASE = import.meta.env.WXT_API_BASE;
const API_URL = import.meta.env.WXT_API_URL;
const t = browser.i18n.getMessage;

function shouldSkip(url: string): boolean {
    try {
        const parsed = new URL(url);
        if (
            parsed.protocol === "chrome-extension:" ||
            parsed.protocol === "moz-extension:"
        )
            return true;
        if (
            parsed.hostname === "127.0.0.1" ||
            parsed.hostname === "localhost" ||
            parsed.hostname === "[::1]" ||
            parsed.hostname === "0.0.0.0"
        )
            return true;
        return false;
    } catch {
        return true;
    }
}

type Status = "idle" | "loading" | "safe" | "gambling" | "bare-ip" | "skipped" | "error";

interface Result {
    category: string;
    gambling_score: number;
    url: string;
    from_list?: string;
}

function App() {
    const [tabUrl, setTabUrl] = useState<string>("");
    const [status, setStatus] = useState<Status>("idle");
    const [result, setResult] = useState<Result | null>(null);
    const [reportState, setReportState] = useState<"idle" | "loading" | "done" | "rate_limited" | "not_classified" | "listed">("idle");

    useEffect(() => {
        browser.tabs
            .query({ active: true, currentWindow: true })
            .then(([tab]) => {
                if (!tab.url || !tab.url.startsWith("http")) {
                    setTabUrl(tab.url || "");
                    setStatus("skipped");
                    return;
                }
                if (shouldSkip(tab.url)) {
                    setTabUrl(tab.url);
                    setStatus("skipped");
                    return;
                }
                setTabUrl(tab.url);
                setStatus("loading");

                const params = new URLSearchParams({ url: tab.url });
                return fetch(`${API_URL}?${params}`);
            })
            .then((res) => res?.json())
            .then((data: Result) => {
                setResult(data);
                if (data.category === "gambling") setStatus("gambling");
                else if (data.category === "bare-ip") setStatus("bare-ip");
                else setStatus("safe");
            })
            .catch(() =>
                setStatus((prev) => (prev === "skipped" ? prev : "error")),
            );
    }, []);

    async function handleReport() {
        setReportState("loading");
        try {
            const res = await fetch(`${API_BASE}/report/false-positive`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    url: tabUrl,
                    gambling_score: result?.gambling_score,
                }),
            });
            const data = await res.json();
            if (data.status === "ok") {
                setReportState("done");
                setTimeout(() => setReportState("idle"), 3000);
            } else if (data.detail?.error === "not_classified") {
                setReportState("not_classified");
                setTimeout(() => setReportState("idle"), 5000);
            } else if (data.detail?.error === "already_blacklisted" || data.detail?.error === "already_whitelisted") {
                setReportState("listed");
                setTimeout(() => setReportState("idle"), 5000);
            } else {
                setReportState("rate_limited");
                setTimeout(() => setReportState("idle"), 5000);
            }
        } catch {
            setReportState("rate_limited");
            setTimeout(() => setReportState("idle"), 5000);
        }
    }

    const scorePercent = result
        ? (result.gambling_score * 100).toFixed(1)
        : null;

    return (
        <div className="w-90 bg-linear-to-b from-white to-gray-50 p-4">
            <div className="flex items-center gap-3 mb-4">
                <div className="size-10 rounded-xl bg-linear-to-br from-indigo-500 to-purple-600 flex items-center justify-center text-white text-lg font-bold shadow-lg shadow-indigo-500/30">
                    GB
                </div>
                <div>
                    <h1 className="text-gray-900 font-semibold text-sm">
                        {t("popup_title")}
                    </h1>
                    <p className="text-gray-500 text-xs">
                        {t("popup_protectionActive")}
                    </p>
                </div>
            </div>

            <div className="bg-gray-50 border border-gray-200 rounded-xl p-3 mb-3">
                <p className="text-xs text-gray-500 mb-1">
                    {t("popup_currentTab")}
                </p>
                <p className="text-sm text-gray-700 truncate">
                    {tabUrl || "—"}
                </p>
            </div>

            {status === "loading" && (
                <div className="bg-gray-50 border border-gray-200 rounded-xl p-4 flex items-center justify-center gap-2">
                    <div className="size-4 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
                    <span className="text-sm text-gray-600">
                        {t("popup_scanning")}
                    </span>
                </div>
            )}

            {status === "error" && (
                <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-center">
                    <p className="text-red-700 text-sm">
                        {t("popup_failedToScan")}
                    </p>
                </div>
            )}

            {status === "skipped" && (
                <div className="bg-amber-50 border border-amber-200 rounded-xl p-4 text-center">
                    <p className="text-amber-800 text-sm font-medium">
                        {t("popup_notScannable")}
                    </p>
                    <p className="text-amber-600/70 text-xs mt-1">
                        {t("popup_notScannableDesc")}
                    </p>
                </div>
            )}

            {status === "bare-ip" && (
                <div className="bg-gray-100 border border-gray-300 rounded-xl p-4 text-center">
                    <p className="text-gray-700 text-sm font-medium">
                        {t("popup_bareIp")}
                    </p>
                    <p className="text-gray-500 text-xs mt-1">
                        {t("popup_bareIpDesc")}
                    </p>
                </div>
            )}

            {status === "safe" && (
                <div className="bg-emerald-50 border border-emerald-200 rounded-xl p-4">
                    <div className="flex items-center gap-2 mb-2">
                        <div className="size-2.5 rounded-full bg-emerald-500 shadow-sm" />
                        <span className="text-emerald-700 font-medium text-sm">
                            {result?.from_list === "whitelist" ? t("popup_allowedByAdmin") : t("popup_safe")}
                        </span>
                    </div>
                    {result?.from_list === "whitelist" ? (
                        <p className="text-xs text-emerald-600/70">
                            {t("popup_allowedByAdminDesc")}
                        </p>
                    ) : scorePercent && (
                        <div>
                            <div className="flex justify-between text-xs text-gray-500 mb-1">
                                <span>{t("popup_gamblingScore")}</span>
                                <span>{scorePercent}%</span>
                            </div>
                            <div className="h-1.5 bg-gray-200 rounded-full overflow-hidden">
                                <div
                                    className="h-full bg-emerald-500 rounded-full transition-all"
                                    style={{ width: `${scorePercent}%` }}
                                />
                            </div>
                        </div>
                    )}
                </div>
            )}

            {status === "gambling" && (
                <div className="bg-red-50 border border-red-200 rounded-xl p-4">
                    <div className="flex items-center gap-2 mb-2">
                        <div className="size-2.5 rounded-full bg-red-500 shadow-sm" />
                        <span className="text-red-700 font-medium text-sm">
                            {result?.from_list === "blacklist" ? t("popup_blockedByAdmin") : t("popup_blocked")}
                        </span>
                    </div>
                    {result?.from_list === "blacklist" ? (
                        <p className="text-xs text-red-600/70">
                            {t("popup_blockedByAdminDesc")}
                        </p>
                    ) : scorePercent && (
                        <div>
                            <div className="flex justify-between text-xs text-gray-500 mb-1">
                                <span>{t("popup_gamblingScore")}</span>
                                <span>{scorePercent}%</span>
                            </div>
                            <div className="h-1.5 bg-gray-200 rounded-full overflow-hidden">
                                <div
                                    className="h-full bg-red-500 rounded-full transition-all"
                                    style={{ width: `${scorePercent}%` }}
                                />
                            </div>
                        </div>
                    )}
                </div>
            )}

            {result?.from_list ? (
                <button
                    disabled
                    className="w-full mt-3 py-2.5 px-4 rounded-xl bg-gray-100 border border-gray-200 text-gray-400 text-sm font-medium cursor-not-allowed flex items-center justify-center gap-2"
                >
                    <span className="text-base">📋</span>
                    {t("reportListed")}
                </button>
            ) : (["safe", "gambling"] as Status[]).includes(status) &&
              reportState === "idle" ? (
                <button
                    onClick={handleReport}
                    className="w-full mt-3 py-2.5 px-4 rounded-xl bg-white border border-gray-200 text-gray-700 text-sm font-medium cursor-pointer transition-all hover:bg-gray-50 flex items-center justify-center gap-2 shadow-xs"
                >
                    <span className="text-base">📋</span>
                    {t("popup_reportFalsePositive")}
                </button>
            ) : (
                <button
                    disabled
                    className="w-full mt-3 py-2.5 px-4 rounded-xl bg-gray-50 border border-gray-200 text-gray-400 text-sm font-medium cursor-not-allowed transition-all flex items-center justify-center gap-2"
                >
                    <span className="text-base">📋</span>
                    {reportState === "loading" && t("reportSending")}
                    {reportState === "done" && t("reportSent")}
                    {reportState === "rate_limited" && t("reportRateLimited")}
                    {reportState === "not_classified" && t("reportNotClassified")}
                    {reportState === "listed" && t("reportListed")}
                    {reportState === "idle" && t("popup_reportFalsePositive")}
                </button>
            )}
        </div>
    );
}

export default App;
