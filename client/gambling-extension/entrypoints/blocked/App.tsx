import { useState } from "react";

const API_BASE = import.meta.env.WXT_API_BASE;
const t = browser.i18n.getMessage;

function App() {
    const params = new URLSearchParams(window.location.search);
    const blockedUrl = params.get("url");
    const gamblingScore = params.get("gambling_score");
    const [reportState, setReportState] = useState<"idle" | "loading" | "done" | "rate_limited">("idle");

    async function handleReport() {
        setReportState("loading");
        try {
            const res = await fetch(`${API_BASE}/report/false-positive`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    url: blockedUrl,
                    gambling_score: Number(gamblingScore),
                }),
            });
            const data = await res.json();
            const isDone = data.status === "ok";
            setReportState(isDone ? "done" : "rate_limited");
            setTimeout(() => setReportState("idle"), isDone ? 3000 : 5000);
        } catch {
            setReportState("rate_limited");
            setTimeout(() => setReportState("idle"), 5000);
        }
    }

    return (
        <div className="min-h-screen bg-linear-to-b from-white to-gray-50 flex items-center justify-center p-4">
            <div className="max-w-md w-full">
                <div className="bg-white rounded-2xl border border-gray-200 p-8 text-center shadow-sm">
                    <div className="size-16 mx-auto mb-4 rounded-2xl bg-red-50 border border-red-200 flex items-center justify-center text-3xl">
                        🚫
                    </div>
                    <h1 className="text-2xl font-bold text-gray-900 mb-1">
                        {t("blocked_title")}
                    </h1>
                    <p className="text-red-600 font-medium text-sm mb-6">
                        {t("blocked_description")}
                    </p>

                    <div className="bg-gray-50 rounded-xl p-4 mb-3 text-left border border-gray-200">
                        <p className="text-xs text-gray-500 mb-1">
                            {t("blocked_url")}
                        </p>
                        <p className="text-sm text-gray-700 break-all">
                            {blockedUrl}
                        </p>
                    </div>

                    <div className="bg-gray-50 rounded-xl p-4 mb-6 text-left border border-gray-200">
                        <p className="text-xs text-gray-500 mb-1">
                            {t("blocked_gamblingScore")}
                        </p>
                        <p className="text-lg font-semibold text-gray-900">
                            {(Number(gamblingScore) * 100).toFixed(1)}%
                        </p>
                        <div className="mt-2 h-1.5 bg-gray-200 rounded-full overflow-hidden">
                            <div
                                className="h-full bg-red-500 rounded-full"
                                style={{
                                    width: `${(Number(gamblingScore) * 100).toFixed(1)}%`,
                                }}
                            />
                        </div>
                    </div>

                    <button
                        onClick={reportState === "idle" ? handleReport : undefined}
                        disabled={reportState !== "idle"}
                        className="w-full py-2.5 px-4 rounded-xl bg-white border border-gray-200 text-gray-700 text-sm font-medium cursor-pointer transition-all hover:bg-gray-50 flex items-center justify-center gap-2 shadow-xs disabled:cursor-not-allowed disabled:bg-gray-50 disabled:text-gray-400 disabled:hover:bg-gray-50"
                    >
                        <span>📋</span>
                        {reportState === "idle" && t("blocked_reportFalsePositive")}
                        {reportState === "loading" && t("reportSending")}
                        {reportState === "done" && t("reportSent")}
                        {reportState === "rate_limited" && t("reportRateLimited")}
                    </button>
                </div>
            </div>
        </div>
    );
}

export default App;
