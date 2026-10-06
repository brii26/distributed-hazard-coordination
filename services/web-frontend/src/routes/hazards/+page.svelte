<script>
  import { onMount } from 'svelte';
  import { goto } from '$app/navigation';
  import { api, base, pollIntervalMs } from '../../lib/api.js';
  import { clearSession, session } from '../../lib/session.svelte.js';

  const SIZES = [10, 25, 50, 100]; // the API refuses more than 100 per page
  const LEVELS = ['AWAS', 'SIAGA', 'WASPADA', 'NORMAL'];
  const PAUSE_MS = 350; // how long typing must stop before the search runs

  let type = $state('SEISMIC');
  let level = $state('');
  let search = $state(''); // what the box holds
  let query = $state(''); // what was last searched, after the pause
  let rows = $state([]);
  let hasMore = $state(false);
  let meta = $state(null);
  let loading = $state(true);
  let error = $state('');
  let pageNo = $state(1);
  let size = $state(25);
  let now = $state(Date.now());
  let live = $state(false); // the push stream is connected
  let seq = 0;

  const offset = $derived((pageNo - 1) * size);

  $effect(() => {
    if (session.checked && !session.loggedIn) goto('/login', { replaceState: true });
  });

  // Run the search once typing has paused, not on every key.
  $effect(() => {
    const typed = search.trim();
    const timer = setTimeout(() => {
      if (typed !== query) {
        query = typed;
        pageNo = 1;
      }
    }, PAUSE_MS);

    return () => clearTimeout(timer);
  });

  // Fetch whenever the tab, filter, search, page size or page changes.
  $effect(() => {
    void [type, level, query, size, pageNo];
    load();
  });

  async function load() {
    const mine = ++seq;
    const params = new URLSearchParams({ type, limit: size, offset });
    if (level) params.set('severity', level);
    if (query) params.set('q', query);

    const { status, body } = await api(`/hazards?${params}`);
    if (mine !== seq) return; // a newer request replaced this one

    loading = false;
    now = Date.now();

    if (status === 200) {
      error = '';
      meta = body.meta;
      if (body.data.length === 0 && pageNo > 1) {
        pageNo = 1; // the page ran out, e.g. after the list changed
        return;
      }
      rows = body.data;
      hasMore = body.page.has_more;
    } else if (status === 401) {
      clearSession();
    } else if (status === 429) {
      error = 'Too many requests. The list will refresh on its own.';
    } else {
      error = body?.error ?? `Could not load hazards (${status || 'no answer'}). Trying again shortly.`;
    }
  }

  function pick(next) {
    type = next;
    level = '';
    search = '';
    query = '';
    pageNo = 1;
    rows = [];
    meta = null;
    loading = true;
  }

  function filter(l) {
    level = level === l ? '' : l;
    pageNo = 1;
  }

  // Push: the server sends an event the moment it exists, and the list reloads so
  // paging, filter and search stay right. EventSource reconnects on its own; events
  // missed while it was down are not replayed, the reload on reconnect picks them up.
  // Polling below stays as the fallback.
  $effect(() => {
    if (!session.loggedIn) return;
    const es = new EventSource(`${base}/hazards/stream?type=${type}`, { withCredentials: true });
    es.onopen = () => {
      live = true;
      load();
    };
    es.onmessage = () => load();
    es.onerror = () => {
      live = false;
    };

    return () => es.close();
  });

  onMount(() => {
    const timer = setInterval(load, pollIntervalMs);
    return () => clearInterval(timer);
  });

  const when = (iso) => new Date(iso).toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' });

  function ago(iso) {
    const s = Math.max(0, Math.round((now - new Date(iso).getTime()) / 1000));
    if (s < 60) return 'just now';
    if (s < 3600) return `${Math.floor(s / 60)} min ago`;
    if (s < 86400) return `${Math.floor(s / 3600)} h ago`;
    return `${Math.floor(s / 86400)} d ago`;
  }

  const filtered = $derived(Boolean(level || query));
</script>

<div class="head">
  <div>
    <h1>Hazards</h1>
    <p class="status" role="status">
      {#if meta}
        Source {meta.source_status}{meta.stale_since ? `, stale since ${when(meta.stale_since)}` : ''}. {live ? 'Live: new hazards appear as they arrive.' : `Live updates are off; this page refreshes every ${pollIntervalMs / 1000} s.`}
      {:else}
        Loading
      {/if}
    </p>
  </div>
  <div class="seg" role="group" aria-label="Hazard type">
    <button aria-pressed={type === 'SEISMIC'} onclick={() => pick('SEISMIC')}>Seismic</button>
    <button aria-pressed={type === 'VOLCANIC'} onclick={() => pick('VOLCANIC')}>Volcanic</button>
  </div>
</div>

<div class="toolbar">
  <label class="search">
    <span class="sr">Search by area</span>
    <input type="search" bind:value={search} placeholder="Search by area, for example Aceh" maxlength="100" autocomplete="off" />
  </label>
  <div class="levels" role="group" aria-label="Filter by severity">
    {#each LEVELS as l (l)}
      <button class="level {l}" aria-pressed={level === l} onclick={() => filter(l)}>
        <span class="dot"></span>{l}
      </button>
    {/each}
  </div>
</div>

{#if error}<p class="note error" role="alert">{error}</p>{/if}

{#if loading}
  <p class="empty" role="status">Loading {type.toLowerCase()} hazards</p>
{:else if rows.length === 0 && !error}
  {#if filtered}
    <p class="empty">
      <strong>Nothing matches</strong>
      No {type.toLowerCase()} hazard fits{level ? ` ${level}` : ''}{query ? ` with "${query}" in the area` : ''}. Change the filter or the search.
    </p>
  {:else}
    <p class="empty">
      <strong>No {type.toLowerCase()} hazards stored yet</strong>
      The Aggregator adds new ones as the source reports them.
    </p>
  {/if}
{:else if rows.length > 0}
  <ol class="feed">
    {#each rows as h (h.hazard_id)}
      <li>
        <details class="event">
          <summary>
            <span class="sev {h.severity ?? 'NORMAL'}">{h.severity ?? 'NO LEVEL'}</span>
            <span class="area">{h.area_name ?? 'Area not given'}</span>
            <span class="ago" title={when(h.occurred_at)}>{ago(h.occurred_at)}</span>
            <span class="kind">{h.hazard_type} from {h.source}</span>
          </summary>
          <dl class="detail">
            <dt>Occurred</dt><dd>{when(h.occurred_at)}</dd>
            <dt>Ingested</dt><dd>{when(h.ingested_at)}</dd>
            <dt>Hazard ID</dt><dd class="mono">{h.hazard_id}</dd>
            <!-- The API leaves these out for Media, so they are drawn only when sent. -->
            {#if 'source_ref_id' in h}<dt>Source ref</dt><dd class="mono">{h.source_ref_id}</dd>{/if}
            {#if 'latitude' in h}
              <dt>Coordinates</dt>
              <dd class="mono">{typeof h.latitude === 'number' ? `${h.latitude.toFixed(4)}, ${h.longitude.toFixed(4)}` : 'not given'}</dd>
            {/if}
            {#if h.attributes}<dt>Attributes</dt><dd><pre>{JSON.stringify(h.attributes, null, 2)}</pre></dd>{/if}
          </dl>
        </details>
      </li>
    {/each}
  </ol>
{/if}

{#if rows.length > 0 || pageNo > 1}
  <div class="pager">
    <span>Showing {offset + 1} to {offset + rows.length}</span>
    <div class="controls">
      <label>
        Per page
        <select bind:value={size} onchange={() => (pageNo = 1)}>
          {#each SIZES as s}<option value={s}>{s}</option>{/each}
        </select>
      </label>
      <button class="quiet" disabled={pageNo <= 1} onclick={() => (pageNo -= 1)}>Previous</button>
      <span>Page {pageNo}</span>
      <button class="quiet" disabled={!hasMore} onclick={() => (pageNo += 1)}>Next</button>
    </div>
  </div>
{/if}
