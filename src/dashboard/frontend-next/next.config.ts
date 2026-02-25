import type { NextConfig } from "next";

const apiTarget = process.env.API_PROXY_TARGET ?? "http://localhost:8000";

const nextConfig: NextConfig = {
  output: "standalone",
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${apiTarget}/api/:path*`,
      },
    ];
  },
};

export default nextConfig;
