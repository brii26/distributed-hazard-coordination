<script>
  import { goto } from '$app/navigation';
  import { page } from '$app/state';
  import { post } from '../../lib/api.js';
  import { loadSession, session } from '../../lib/session.svelte.js';

  let email = $state('');
  let password = $state('');
  let error = $state('');
  let busy = $state(false);

  $effect(() => {
    if (session.loggedIn) goto('/hazards', { replaceState: true });
  });

  async function submit(event) {
    event.preventDefault();
    busy = true;
    error = '';
    const { status } = await post('/auth/login', { email, password });
    busy = false;
    if (status === 200) {
      await loadSession();
      goto('/hazards');
      return;
    }
    error = status === 401 ? 'Wrong email or password.' : `Login failed (${status || 'no answer'}). Try again.`;
  }
</script>

<div class="split">
  <section class="lede">
    <h1>Seismic and volcanic hazards, as they are reported.</h1>
    <p>Reports from BMKG and PVMBG in one list. What you can see depends on your role.</p>
  </section>

  <section class="card">
    <h2>Log in</h2>

    {#if page.url.searchParams.has('registered')}
      <p class="note" role="status">Account created. You can log in now.</p>
    {:else if page.url.searchParams.has('loggedout')}
      <p class="note" role="status">You are logged out.</p>
    {/if}

    <form onsubmit={submit}>
      <label>Email <input type="email" bind:value={email} required autocomplete="username" /></label>
      <label>Password <input type="password" bind:value={password} required autocomplete="current-password" /></label>
      <button disabled={busy}>Log in</button>
    </form>
    {#if error}<p class="note error" role="alert">{error}</p>{/if}

    <p class="switch">No account yet? <a href="/register">Register</a></p>
  </section>
</div>
