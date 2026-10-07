// Baked in at build time from the Dockerfile build args.
export const base = import.meta.env.VITE_CLIENT_FACING_API_BASE_URL;
export const pollIntervalMs = Number(import.meta.env.VITE_FRONTEND_POLL_INTERVAL_MS);

// Calls the API with the HttpOnly session cookies attached by the browser, so no token is read or stored here. status 0 means no answer.
export async function api(path, options = {}) {
  try {
    const res = await fetch(base + path, { credentials: 'include', ...options });
    let body = null;
    try {
      body = await res.json();
    } catch {
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
