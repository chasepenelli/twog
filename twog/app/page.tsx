// The homepage IS the v4 "PROOF" experience. The prior landing now lives at /legacy, and /v4 still
// renders the same component for side-by-side reference.
import V4 from './v4/page';

export const metadata = {
  title: 'TWOG — A Living Research Engine',
  description: 'Real proof for real cancer breakthroughs — tested in the open, and worth believing.',
};

export default function Home() {
  return <V4 />;
}
