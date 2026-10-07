import { api } from './api.js';

// Asks the API who the session belongs to (GET /me), since HttpOnly cookies can't be read. The role only decides what is shown, the API enforces it.
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
