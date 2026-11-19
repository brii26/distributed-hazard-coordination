<script>
  import { onMount } from 'svelte';
  import { goto } from '$app/navigation';
  import { api } from '../../lib/api.js';
  import { clearSession, session } from '../../lib/session.svelte.js';

  const RANGES = { '1h': 3600, '6h': 21600, '24h': 86400, '7d': 604800 };
  const CATEGORIES = ['earthquake', 'tsunami', 'volcano', 'wildfire', 'coastal'];
  const COLORS = { earthquake: '#4e79a7', tsunami: '#59a14f', volcano: '#e15759', wildfire: '#f28e2b', coastal: '#b07aa1' };
  const LEVELS = ['AWAS', 'SIAGA', 'WASPADA', 'NORMAL'];
  const TOP_AREAS = 20;
  const REFRESH_MS = 30000;
  const W = 640;
  const H = 220;
  const PAD = { l: 36, r: 8, t: 8, b: 20 };

  let range = $state('24h');
  let data = $state(null);
  let loading = $state(true);
  let error = $state('');
  let seq = 0;

  $effect(() => {
    if (session.checked && !session.loggedIn) goto('/login', { replaceState: true });
  });

  $effect(() => {
    void range;
    load();
  });

  async function load() {
    const mine = ++seq;
    const { status, body } = await api(`/analytics?range=${range}`);
    if (mine !== seq) return;

    loading = false;
    if (status === 200) {
      error = '';
      data = body;
    } else if (status === 401) {
      clearSession();
    } else {
      // Numbers of another range must not sit under the button that was just pressed.
      if (data && data.range !== range) data = null;
      const retry = status === 429 ? 'Too many requests, will retry.' : 'Cannot refresh.';
      error = data ? `${retry} Showing the last numbers.` : `${retry} No numbers for ${range}.`;
    }
  }

  onMount(() => {
    const timer = setInterval(() => {
      if (document.visibilityState === 'visible') load();
    }, REFRESH_MS);
    return () => clearInterval(timer);
  });

  // Every bucket from the window start to as_of, so quiet buckets show as zero.
  const chart = $derived.by(() => {
    if (!data) return null;
    const b = data.bucket_seconds;
    const end = Math.floor(new Date(data.as_of).getTime() / 1000);
    const first = Math.floor((end - RANGES[data.range]) / b) * b;
    const starts = [];
    for (let t = first; t <= end; t += b) starts.push(t);

    const counts = Object.fromEntries(CATEGORIES.map((c) => [c, new Map()]));
    for (const s of data.series) counts[s.category]?.set(s.bucket_start, s.count);

    let max = 1;
    for (const c of CATEGORIES) for (const t of starts) max = Math.max(max, counts[c].get(t) ?? 0);

    const x = (i) => PAD.l + (i * (W - PAD.l - PAD.r)) / Math.max(1, starts.length - 1);
    const y = (v) => H - PAD.b - (v * (H - PAD.t - PAD.b)) / max;
    const lines = CATEGORIES.map((c) => ({
      c,
      total: starts.reduce((n, t) => n + (counts[c].get(t) ?? 0), 0),
      points: starts.map((t, i) => `${x(i).toFixed(1)},${y(counts[c].get(t) ?? 0).toFixed(1)}`).join(' ')
    }));

    return { lines, max, y, x, first: starts[0], last: starts[starts.length - 1] };
  });

  const total = $derived(data ? data.by_source.reduce((n, s) => n + s.count, 0) : 0);
  const severities = $derived(
    data ? [...LEVELS, ''].map((k) => ({ key: k, count: data.by_severity.find((s) => s.key === k)?.count ?? 0 })) : []
  );
  const areas = $derived.by(() => {
    if (!data) return [];
    const sorted = data.by_area.filter((a) => a.key !== '').slice().sort((a, b) => b.count - a.count);
    const top = sorted.slice(0, TOP_AREAS);
    const other = sorted.slice(TOP_AREAS).reduce((n, a) => n + a.count, 0) + (data.by_area.find((a) => a.key === '')?.count ?? 0);
    return other > 0 ? [...top, { key: 'Other or not given', count: other }] : top;
  });
  const maxArea = $derived(Math.max(1, ...areas.map((a) => a.count)));
  const maxSeverity = $derived(Math.max(1, ...severities.map((s) => s.count)));

  const when = (sec) => new Date(sec * 1000).toLocaleString([], { dateStyle: 'short', timeStyle: 'short' });
</script>

<div class="head">
  <div>
    <h1>Analytics</h1>
    <p class="status" role="status">
      {#if data}{total} events in the last {data.range}, as of {new Date(data.as_of).toLocaleTimeString()}. Counted by ingestion time.{:else}Loading{/if}
    </p>
  </div>
  <div class="seg" role="group" aria-label="Time range">
    {#each Object.keys(RANGES) as r (r)}
      <button aria-pressed={range === r} onclick={() => (range = r)}>{r}</button>
    {/each}
  </div>
</div>

{#if data?.partial}
  <p class="note error" role="alert">
    Numbers incomplete: some data is unavailable. No copy of {data.unavailable_units.join(', ')} could be reached, so those events are not counted.
  </p>
{:else if data?.degraded}
  <p class="note error" role="alert">
    Numbers incomplete: a node is down or still catching up, so the numbers may be too low.
  </p>
{/if}
{#if error}<p class="note error" role="alert">{error}</p>{/if}
<p><button class="quiet" onclick={load}>Refresh</button></p>

{#if loading}
  <p class="empty" role="status">Loading analytics</p>
{:else if chart}
  <h2>Events per hazard type</h2>
  <svg viewBox="0 0 {W} {H}" role="img" aria-label="Events per hazard type over time" class="chart">
    {#each [0, 0.5, 1] as f}
      <line x1={PAD.l} x2={W - PAD.r} y1={chart.y(chart.max * f)} y2={chart.y(chart.max * f)} class="grid" />
      <text x={PAD.l - 4} y={chart.y(chart.max * f) + 4} text-anchor="end" class="tick">{Math.round(chart.max * f)}</text>
    {/each}
    <text x={PAD.l} y={H - 4} class="tick">{when(chart.first)}</text>
    <text x={W - PAD.r} y={H - 4} text-anchor="end" class="tick">{when(chart.last)}</text>
    {#each chart.lines as l (l.c)}
      <polyline points={l.points} fill="none" stroke={COLORS[l.c]} stroke-width="2" />
    {/each}
  </svg>
  <ul class="legend">
    {#each chart.lines as l (l.c)}
      <li><span class="swatch" style="background:{COLORS[l.c]}"></span>{l.c} {l.total}</li>
    {/each}
  </ul>
  <p class="status">Tsunami means a seismic event with potential_tsunami set, and is not also counted as earthquake. Wildfire stays at zero until a satellite feed is stored.</p>

  <h2>By severity</h2>
  <table class="bars">
    <tbody>
      {#each severities as s (s.key)}
        <tr>
          <th scope="row">{s.key || 'Not given'}</th>
          <td class="n">{s.count}</td>
          <td><span class="bar" style="width:{(s.count / maxSeverity) * 100}%"></span></td>
        </tr>
      {/each}
    </tbody>
  </table>

  <h2>By area</h2>
  {#if areas.length === 0}
    <p class="empty">No events in this range.</p>
  {:else}
    <table class="bars">
      <tbody>
        {#each areas as a (a.key)}
          <tr>
            <th scope="row">{a.key}</th>
            <td class="n">{a.count}</td>
            <td><span class="bar" style="width:{(a.count / maxArea) * 100}%"></span></td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
{/if}

<style>
  h2 { margin-top: 2rem; }
  .chart { width: 100%; height: auto; margin-top: 1rem; }
  .grid { stroke: var(--rule); stroke-width: 1; }
  .tick { fill: var(--muted); font-size: 10px; }
  .legend { display: flex; flex-wrap: wrap; gap: .5rem 1.25rem; list-style: none; padding: 0; margin: .5rem 0; }
  .swatch { display: inline-block; width: .8rem; height: .8rem; border-radius: 2px; margin-right: .4rem; }
  .bars { width: 100%; border-collapse: collapse; }
  .bars th { text-align: left; font-weight: 500; width: 14rem; padding: .3rem .5rem .3rem 0; }
  .bars .n { width: 4rem; text-align: right; padding-right: 1rem; font-variant-numeric: tabular-nums; }
  .bar { display: block; height: .7rem; background: var(--accent); border-radius: 2px; }
</style>
