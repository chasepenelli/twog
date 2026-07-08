// The homepage IS the v4 "PROOF" experience (app/v4/page.tsx). /v4 renders the same component.
import V4 from './v4/page';

export const metadata = {
  title: 'TWOG — A Living Research Engine',
  description: 'Real proof for real cancer breakthroughs — tested in the open, and worth believing.',
};

export default function Home() {
  return <V4 />;
}
