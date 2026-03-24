# Build stage
FROM node:18-alpine AS builder

WORKDIR /app

# Install system dependencies needed for some npm packages
RUN apk add --no-cache python3 make g++ git curl

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci

# Copy source code
COPY . .

# Build the Vite project
RUN npm run build

# Serve stage
FROM nginx:alpine

# Copy built assets
COPY --from=builder /app/dist /usr/share/nginx/html

# Nginx configuration for single-page application fallback and API proxy
RUN echo "server { \
    listen 3737; \
    location / { \
        root /usr/share/nginx/html; \
        index index.html; \
        try_files \$uri \$uri/ /index.html; \
    } \
    location /health { \
        proxy_pass http://archon-server:8181/health; \
        proxy_http_version 1.1; \
        proxy_set_header Host \$host; \
    } \
    location /api/ { \
        proxy_pass http://archon-server:8181/api/; \
        proxy_http_version 1.1; \
        proxy_set_header Upgrade \$http_upgrade; \
        proxy_set_header Connection 'upgrade'; \
        proxy_set_header Host \$host; \
        proxy_cache_bypass \$http_upgrade; \
    } \
}" > /etc/nginx/conf.d/default.conf

EXPOSE 3737

CMD ["nginx", "-g", "daemon off;"]
