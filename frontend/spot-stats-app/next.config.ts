/** @type {import('next').NextConfig} */
const nextConfig = {
  async rewrites() {
    return [
      {
        source: '/api/:path*',
        destination: `${process.env.BACKEND_API_URL}/api/:path*`, // Use the build-time environment variable
      }
    ]
  }
}

module.exports = nextConfig
