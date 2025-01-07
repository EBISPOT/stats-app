# Build stage
FROM node:20-slim AS builder
WORKDIR /app
ARG BACKEND_API_URL
ENV BACKEND_API_URL=${BACKEND_API_URL}
COPY package*.json .
RUN npm install --legacy-peer-deps
COPY . .
RUN echo '{ "extends": "next/core-web-vitals" }' > .eslintrc.json
RUN npm run build

# Production stage
FROM node:20-slim
WORKDIR /app
COPY --from=builder /app/package*.json .
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public
COPY --from=builder /app/node_modules ./node_modules
EXPOSE 3000
CMD ["npm", "run", "start"]