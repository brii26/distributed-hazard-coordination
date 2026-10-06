<script>
  import { goto } from '$app/navigation';
  import { post } from '../../lib/api.js';
  import { session } from '../../lib/session.svelte.js';

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
    const { status } = await post('/auth/register', { email, password });
    busy = false;
    if (status === 201) {
      goto('/login?registered=1');
      return;
    }
    error =
      status === 409
        ? 'That email is already registered.'
        : status === 400
          ? 'Check the email and password.'
          : `Registration failed (${status || 'no answer'}). Try again.`;
  }
</script>

<div class="split">
  <section class="lede">
    <h1>Register for read access.</h1>
    <p>Self-service accounts are Media accounts: each hazard shows its type, severity, area and times. Field teams and operators get their accounts from an Operator.</p>
  </section>

  <section class="card">
    <h2>Create a Media account</h2>
    <form onsubmit={submit}>
      <label>Email <input type="email" bind:value={email} required autocomplete="username" /></label>
      <label>Password <input type="password" bind:value={password} required autocomplete="new-password" /></label>
      <button disabled={busy}>Register</button>
    </form>
    {#if error}<p class="note error" role="alert">{error}</p>{/if}

    <p class="switch">Already registered? <a href="/login">Log in</a></p>
  </section>
</div>
