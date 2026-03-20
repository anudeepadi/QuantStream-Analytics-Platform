import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: (process.env.NEXT_OUTPUT_MODE as "export" | "standalone") || "export",
  // "export"     — static HTML for Vercel (default)
  // "standalone" — Node.js server for Docker (set NEXT_OUTPUT_MODE=standalone)
  // API calls go directly to the backend via NEXT_PUBLIC_API_BASE_URL (build time).
};

export default nextConfig;
