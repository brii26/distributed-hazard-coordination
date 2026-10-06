// Baked in at build time (Dockerfile build args), see doc/TRD_M1.md 9.
const base = import.meta.env.VITE_CLIENT_FACING_API_BASE_URL;
export const pollIntervalMs = Number(import.meta.env.VITE_FRONTEND_POLL_INTERVAL_MS);

// The session lives in HttpOnly cookies the browser attaches itself (credentials:
// "include"); this code never reads or stores a token. status 0 means the request
// never got an answer.
export async function api(path, options = {}) {
  try {
    const res = await fetch(base + path, { credentials: 'include', ...options });
    let body = null;
    try {
      body = await res.json();
    } catch {
      // 201 and 204 carry no body
    }
    return { status: res.status, body };
  } catch {
    return { status: 0, body: null };
  }
}

export const post = (path, data) =>
  api(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  });
