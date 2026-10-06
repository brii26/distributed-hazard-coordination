<script>
  import { goto } from '$app/navigation';
  import { session } from '../lib/session.svelte.js';

  $effect(() => {
    if (session.checked) goto(session.loggedIn ? '/hazards' : '/login', { replaceState: true });
  });
</script>

<p class="status" role="status">Loading</p>
