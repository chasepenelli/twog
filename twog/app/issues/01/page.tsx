/**
 * /issues/01 — Article page (pre-release hold).
 *
 * Before Friday, April 24, 2026 at 12:00 noon MST (18:00 UTC), this page
 * shows the teaser card: header title + portrait + countdown. After that
 * moment, we swap in the full article body. For now, teaser only.
 */

import ArticleTeaser from '@/components/home/ArticleTeaser';

export const metadata = {
  title: 'Three Drugs That Might Let Dogs Stay Home — TWOG Issue 01',
  description:
    'The first issue of TWOG: three oral drugs, stacked, that could let a dog with hemangiosarcoma stay home instead of going through surgery and IV chemo. Releases Friday April 24 at noon MST.',
};

export default function Issue01Page() {
  return (
    <div style={{ paddingTop: 80 }}>
      <ArticleTeaser />
    </div>
  );
}
