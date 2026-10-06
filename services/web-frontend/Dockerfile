# syntax=docker/dockerfile:1
FROM nginx:1.27-alpine

# Static files: the two settings are baked into config.js at build time.
ARG CLIENT_FACING_API_BASE_URL=
ARG FRONTEND_POLL_INTERVAL_MS=5000

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html app.js style.css config.js /usr/share/nginx/html/
# COPY keeps the host file modes; nginx's worker needs to read them.
RUN chmod -R a+rX /usr/share/nginx/html \
 && sed -i "s|__CLIENT_FACING_API_BASE_URL__|${CLIENT_FACING_API_BASE_URL}|; s|__FRONTEND_POLL_INTERVAL_MS__|${FRONTEND_POLL_INTERVAL_MS}|" /usr/share/nginx/html/config.js
