# syntax=docker/dockerfile:1
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .

# Baked into the static files at build time.
ARG CLIENT_FACING_API_BASE_URL=/api
ARG FRONTEND_POLL_INTERVAL_MS=5000
ENV VITE_CLIENT_FACING_API_BASE_URL=$CLIENT_FACING_API_BASE_URL \
    VITE_FRONTEND_POLL_INTERVAL_MS=$FRONTEND_POLL_INTERVAL_MS
RUN npm run build

FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/build /usr/share/nginx/html
# Keep the build stage's modes so the nginx worker can read the files.
RUN chmod -R a+rX /usr/share/nginx/html
