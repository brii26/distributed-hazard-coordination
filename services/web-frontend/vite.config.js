import adapter from '@sveltejs/adapter-static';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

// A static single-page app (doc/TRD_M1.md 9): nginx serves the files and falls back
// to index.html, and the browser does all the rendering.
export default defineConfig({
  plugins: [sveltekit({ adapter: adapter({ fallback: 'index.html' }) })]
});
