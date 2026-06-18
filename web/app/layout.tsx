import type { Metadata } from "next";
import { IBM_Plex_Sans, IBM_Plex_Serif, IBM_Plex_Mono } from "next/font/google";

import "./globals.css";
import { getPrincipal } from "@/lib/auth/server";
import { SessionProvider } from "@/components/providers/SessionProvider";
import { BrandShell } from "@/components/shell/brand-shell";
import { Toaster } from "@/components/ui/toaster";

const sans = IBM_Plex_Sans({
  subsets: ["latin"],
  weight: ["400", "500", "600", "700"],
  variable: "--font-sans",
  display: "swap",
});
const serif = IBM_Plex_Serif({
  subsets: ["latin"],
  weight: ["400", "500"],
  style: ["normal", "italic"],
  variable: "--font-serif",
  display: "swap",
});
const mono = IBM_Plex_Mono({
  subsets: ["latin"],
  weight: ["400", "500", "600"],
  variable: "--font-mono",
  display: "swap",
});

export const metadata: Metadata = {
  title: {
    default: "twog — research state",
    template: "%s · twog",
  },
  description:
    "An engine that tries to be wrong. Autonomous comparative oncology — canine HSA × human AS. Nothing auto-promotes.",
};

export default async function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  // Resolve the twog principal on the SERVER (trustworthy role + scopes), hand to the client.
  const principal = await getPrincipal();

  return (
    <html lang="en" className={`${sans.variable} ${serif.variable} ${mono.variable}`}>
      <body>
        <SessionProvider principal={principal}>
          <BrandShell>{children}</BrandShell>
          <Toaster />
        </SessionProvider>
      </body>
    </html>
  );
}
