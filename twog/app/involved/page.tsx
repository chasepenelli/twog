import Link from 'next/link';
import '../v4/v4.css';
import '../v2/v2.css';
import '../detail.css';
import './involved.css';
import { SuggestForm } from '@/components/v4/SuggestForm';
import { EmailGate } from '@/components/v4/EmailGate';

export const metadata = {
  title: 'Get involved — TWOG',
  description: 'From just watching to running your own experiments — there’s a way in at every level.',
};

const RUNGS: [string, string, string, string, string][] = [
  ['Watch', 'no signup', 'Open the live ledger and watch the engine work — every propose, lock, dock, and verdict.', 'Watch it work →', '/'],
  ['Follow', '1 click', 'Get the next result in your inbox when an experiment resolves.', 'Follow the build →', 'https://pushingc.substack.com/subscribe'],
  ['Suggest', 'low', 'Name a drug, a target, or a question. We’ll put it to the test in public.', 'Suggest below ↓', '#suggest'],
  ['Contribute', 'hands on', 'Inspect a real record, pressure-test a result, return structured work.', 'Inspect the records →', '/candidates'],
  ['Run an agent', 'all in', 'Spin up your own experiment under the same rules.', 'See the architecture →', '/architecture'],
];

export default function InvolvedPage() {
  return (
    <div className="v4-shell">
      <div className="v4-grain" />
      <div className="v4-detail">
        <nav className="v4-dnav">
          <Link href="/" className="v4-dnav__home">twog</Link>
          <Link href="/runs">Runs</Link>
          <Link href="/evidence">Evidence</Link>
          <span className="v4-dnav__spacer" />
        </nav>

        <p className="v4-kick">Get involved</p>
        <h1 className="v4-dh1">Pick your altitude.</h1>
        <p className="v4-lead">
          From just watching to running your own experiments — there’s a way in at every level. All of it
          is public; none of it needs an account.
        </p>

        <div className="v4-ladder2">
          {RUNGS.map(([title, tier, body, cta, href]) => (
            <article className="v4-rung2" key={title}>
              <span className="v4-rung2__tier">{tier}</span>
              <strong>{title}</strong>
              <p>{body}</p>
              {href.startsWith('http') ? (
                <a href={href} target="_blank" rel="noopener noreferrer">{cta}</a>
              ) : href.startsWith('#') ? (
                <a href={href}>{cta}</a>
              ) : (
                <Link href={href}>{cta}</Link>
              )}
            </article>
          ))}
        </div>

        <section className="v4-sec2" id="suggest">
          <div className="v4-dh2">Suggest an experiment</div>
          <p className="v4-lead">
            Got a hypothesis? Name a drug, a target, or a plain question, and the engine will put it to a
            pre-registered kill-test — in public, win or dead end.
          </p>
          <SuggestForm />
        </section>

        <section className="v4-sec2">
          <div className="v4-dh2">Get every verdict</div>
          <EmailGate
            mode="newsletter"
            source="involved"
            heading="The results, in your inbox."
            blurb="One email when an experiment resolves — the verdict, and how you can check it. No noise, and an invite to sign in once real accounts are ready."
          />
        </section>
      </div>
    </div>
  );
}
