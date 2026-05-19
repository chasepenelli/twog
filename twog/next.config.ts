import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  async redirects() {
    return [
      // Legacy internal redirects — retained so old links still route.
      { source: '/discoveries', destination: '/research', permanent: true },
      { source: '/findings', destination: '/validation', permanent: true },
      { source: '/dashboard', destination: '/', permanent: true },

      // /schematic, /treatments, /research are all live pages in the new
      // five-item nav (Article / Early Detection / Treatments / Research /
      // Schematic). No redirects needed here. /validation stays retired —
      // route it to the article teaser.
      { source: '/validation', destination: '/issues/01', statusCode: 301 },
    ];
  },
};

export default nextConfig;
