import { api } from './api.js';

// The page cannot read the HttpOnly cookies, so it asks the API who the session
// belongs to (GET /me). The role only decides what the page shows; the API still
// enforces every role check itself.
export const session = $state({ checked: false, loggedIn: false, role: '', email: '' });

export async function loadSession() {
  const { status, body } = await api('/me');
  session.loggedIn = status === 200;
  session.role = status === 200 ? body.role : '';
  session.email = status === 200 ? body.email : '';
  session.checked = true;
}

export function clearSession() {
  session.loggedIn = false;
  session.role = '';
  session.email = '';
}
