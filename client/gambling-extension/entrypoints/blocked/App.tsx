const t = browser.i18n.getMessage;

function App() {
    const params = new URLSearchParams(window.location.search);
    const blockedUrl = params.get("url");
    const gamblingScore = params.get("gambling_score");

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

                    <button className="w-full py-2.5 px-4 rounded-xl bg-white border border-gray-200 text-gray-700 text-sm font-medium cursor-pointer transition-all hover:bg-gray-50 flex items-center justify-center gap-2 shadow-xs">
                        <span>📋</span>
                        {t("blocked_reportFalsePositive")}
                    </button>
                </div>
            </div>
        </div>
    );
}

export default App;
