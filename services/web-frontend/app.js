// Client-side rendered dashboard for the Client-Facing API (doc/TRD_M1.md 9). The
// session lives in HttpOnly cookies the browser attaches itself (credentials:
// "include"); this code never reads or stores a token. Everything from the API is
// written with textContent, never as HTML.
const { apiBase, pollIntervalMs } = window.CONFIG;
const $ = (id) => document.getElementById(id);

const SUMMARY = ["hazard_id", "source", "hazard_type", "severity", "area_name", "occurred_at", "ingested_at"];
const RAW = ["source_ref_id", "latitude", "longitude", "attributes"];

let hazardType = "SEISMIC";
let timer = null;

async function api(path, options = {}) {
  const res = await fetch(apiBase + path, { credentials: "include", ...options });
  let body = null;
  try { body = await res.json(); } catch { /* 201/204 have no body */ }
  return { status: res.status, body };
}

const post = (path, data) =>
  api(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data) });

function say(text, isError = false) {
  $("message").textContent = text;
  $("message").className = isError ? "error" : "";
}

function show(view) {
  for (const v of ["login", "register", "hazards"]) $(`view-${v}`).hidden = v !== view;
  $("logout").hidden = view !== "hazards";
  clearInterval(timer);
  if (view === "hazards") {
    refresh();
    timer = setInterval(refresh, pollIntervalMs);
  }
}

function route() {
  if (location.hash === "#register") { say(""); show("register"); } else { say(""); show("login"); }
}

function cell(row, text) {
  const td = document.createElement("td");
  td.textContent = text;
  row.append(td);
}

function render(data) {
  // The API only sends the raw fields to roles allowed to see them, so a Media
  // response has no such keys and no such columns are drawn.
  const cols = SUMMARY.concat(data.length && "latitude" in data[0] ? RAW : []);
  const head = document.createElement("tr");
  for (const c of cols) {
    const th = document.createElement("th");
    th.textContent = c;
    head.append(th);
  }
  $("hazards").tHead.replaceChildren(head);

  const rows = data.map((e) => {
    const tr = document.createElement("tr");
    for (const c of cols) cell(tr, e[c] === null || e[c] === undefined ? "-" : typeof e[c] === "object" ? JSON.stringify(e[c]) : e[c]);
    return tr;
  });
  $("hazards").tBodies[0].replaceChildren(...rows);
}

async function refresh() {
  const { status, body } = await api(`/hazards?type=${hazardType}`);
  if (status === 401) { say("Session ended, please log in.", true); show("login"); return; }
  if (status !== 200) { say(body?.error || `Could not load hazards (${status}).`, true); return; }

  say("");
  render(body.data);
  const m = body.meta;
  $("source-status").textContent = `source ${m.source_status}${m.stale_since ? `, stale since ${m.stale_since}` : ""}`;
}

function onSubmit(id, handler) {
  $(id).addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const form = ev.target;
    await handler(Object.fromEntries(new FormData(form)), form);
  });
}

onSubmit("login-form", async (data, form) => {
  const { status } = await post("/auth/login", data);
  if (status !== 200) { say(status === 401 ? "Wrong email or password." : `Login failed (${status}).`, true); return; }
  form.reset();
  say("");
  show("hazards");
});

onSubmit("register-form", async (data, form) => {
  const { status } = await post("/auth/register", data);
  if (status === 201) { form.reset(); location.hash = "#login"; say("Registered. You can log in now."); return; }
  say(status === 409 ? "That email is already registered." : status === 400 ? "Check the email and password." : `Registration failed (${status}).`, true);
});

onSubmit("account-form", async (data, form) => {
  const { status } = await post("/accounts", data);
  if (status === 201) { form.reset(); say(`Created a ${data.role} account.`); return; }
  say(status === 403 ? "Only an Operator can create accounts." : status === 409 ? "That email is already registered." : `Could not create the account (${status}).`, true);
});

$("logout").addEventListener("click", async () => {
  const { status } = await post("/auth/logout", {});
  if (status >= 500) { say("Logout failed, try again.", true); return; }
  location.hash = "#login";
  show("login");
  say("Logged out.");
});

for (const b of document.querySelectorAll(".tab")) {
  b.addEventListener("click", () => { hazardType = b.dataset.type; setTabs(); refresh(); });
}

function setTabs() {
  for (const b of document.querySelectorAll(".tab")) b.setAttribute("aria-pressed", String(b.dataset.type === hazardType));
}

window.addEventListener("hashchange", () => { if (!$("view-hazards").hidden) return; route(); });

// A reload keeps the cookies, so a working session goes straight to the hazards.
(async () => {
  setTabs();
  const { status } = await api(`/hazards?type=${hazardType}`);
  if (status === 200) show("hazards"); else route();
})();
