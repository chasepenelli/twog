import type { Metadata } from 'next';
import './globals.css';
import Nav from '@/components/Nav';
import Footer from '@/components/Footer';
import SmoothScroll from '@/components/SmoothScroll';

export const metadata: Metadata = {
  title: 'TWOG — A Living Research Engine',
  description:
    'A living research engine turning fragmented cancer evidence into inspectable candidate records, citation audits, methods, and human-reviewed validation paths.',
  openGraph: {
    title: 'TWOG — A Living Research Engine',
    description: 'Inspectable research records for canine hemangiosarcoma and related translational oncology.',
    type: 'website',
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        <SmoothScroll>
          <Nav />
          <main>{children}</main>
          <Footer />
        </SmoothScroll>
      </body>
    </html>
  );
}
