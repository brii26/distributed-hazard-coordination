<script>
  import { goto } from '$app/navigation';
  import { post } from '../../lib/api.js';
  import { session } from '../../lib/session.svelte.js';

  const ROLES = [
    { name: 'Lapangan', sees: 'Field teams. Every field, including coordinates and source attributes.' },
    { name: 'Operator', sees: 'BNPB operations. Every field, and can create accounts.' },
    { name: 'Media', sees: 'Press. Summary fields only.' }
  ];

  let email = $state('');
  let password = $state('');
  let role = $state('Lapangan');
  let message = $state('');
  let failed = $state(false);
  let busy = $state(false);

  $effect(() => {
    if (!session.checked) return;
    if (!session.loggedIn) goto('/login', { replaceState: true });
    else if (session.role !== 'Operator') goto('/hazards', { replaceState: true });
  });

  async function submit(event) {
    event.preventDefault();
    busy = true;
    message = '';
    const { status } = await post('/accounts', { email, password, role });
    busy = false;
    failed = status !== 201;
    if (status === 201) {
      message = `Created a ${role} account for ${email}.`;
      email = '';
      password = '';
      return;
    }
    message =
      status === 403
        ? 'Only an Operator can create accounts. This session has a different role.'
        : status === 409
          ? 'That email is already registered.'
          : status === 400
            ? 'Check the email, password and role.'
            : `Could not create the account (${status || 'no answer'}).`;
  }
</script>

<div class="split">
  <section class="lede">
    <h1>Create an account.</h1>
    <p>Operators only. Choose the role the new person needs, then hand over the email and password.</p>
  </section>

  <section class="card">
    <form onsubmit={submit}>
      <label>Email <input type="email" bind:value={email} required autocomplete="off" /></label>
      <label>Password <input type="password" bind:value={password} required autocomplete="new-password" /></label>
      <fieldset>
        <legend>Role</legend>
        <div class="roles">
          {#each ROLES as r (r.name)}
            <label class="role">
              <input type="radio" name="role" value={r.name} bind:group={role} />
              <span><strong>{r.name}</strong><span>{r.sees}</span></span>
            </label>
          {/each}
        </div>
      </fieldset>
      <button disabled={busy}>Create account</button>
    </form>
    {#if message}<p class="note" class:error={failed} role={failed ? 'alert' : 'status'}>{message}</p>{/if}
  </section>
</div>
