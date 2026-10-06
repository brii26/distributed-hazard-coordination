<script>
  import '../app.css';
  import { onMount } from 'svelte';
  import { goto } from '$app/navigation';
  import { page } from '$app/state';
  import { post } from '../lib/api.js';
  import { clearSession, loadSession, session } from '../lib/session.svelte.js';

  let { children } = $props();

  // Cookies survive a reload, so one request tells whether the session is still good.
  onMount(loadSession);

  async function logout() {
    await post('/auth/logout', {});
    clearSession();
    goto('/login?loggedout=1');
  }

  const current = (path) => (page.url.pathname === path ? 'page' : undefined);
</script>

<div class="shell">
  <header class="masthead">
    <a class="brand" href="/hazards">Hazard Coordination</a>
    <nav aria-label="Main">
      {#if session.loggedIn}
        <a href="/hazards" aria-current={current('/hazards')}>Hazards</a>
        {#if session.role === 'Operator'}
          <a href="/accounts" aria-current={current('/accounts')}>Accounts</a>
        {/if}
        <button class="quiet" onclick={logout}>Log out</button>
      {:else if session.checked}
        <a href="/login" aria-current={current('/login')}>Log in</a>
        <a href="/register" aria-current={current('/register')}>Register</a>
      {/if}
    </nav>
  </header>

  <main>
    {@render children()}
  </main>
</div>
