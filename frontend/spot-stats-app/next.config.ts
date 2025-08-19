/** @type {import('next').NextConfig} */
const nextConfig = {
  async rewrites() {
    const backendUrl = process.env.BACKEND_API_URL || 'http://localhost:8000';
    return [
      {
        source: '/api/:path*',
        destination: `${backendUrl}/api/:path*`,
      }
    ]
  },
  experimental: {
    proxyTimeout: 900000, // 15 minutes timeout for proxied requests
  },
  serverRuntimeConfig: {
    // Server-side configuration
    requestTimeout: 900000, // 15 minutes
  }
}

module.exports = nextConfig
