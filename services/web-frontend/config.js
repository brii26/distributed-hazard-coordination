// Replaced at image build time (see Dockerfile): CLIENT_FACING_API_BASE_URL is "" for
// the same origin (nginx proxies the API), or another origin's URL.
window.CONFIG = { apiBase: "__CLIENT_FACING_API_BASE_URL__", pollIntervalMs: Number("__FRONTEND_POLL_INTERVAL_MS__") };
