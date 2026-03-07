import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "export",
  // Static export: no server-side rewrites. API calls go directly to the backend
  // via NEXT_PUBLIC_API_BASE_URL (set at build time). Backend must allow CORS.
};

export default nextConfig;
