import Link from 'next/link';
import { notFound } from 'next/navigation';
import { listKnownSkills, readSkillContent } from '@/lib/skill-content';
import styles from './skill.module.css';

export const dynamic = 'force-static';

export async function generateStaticParams() {
  return listKnownSkills().map(({ slug }) => ({ name: slug }));
}

interface PageProps {
  params: Promise<{ name: string }>;
}

export async function generateMetadata({ params }: PageProps) {
  const { name } = await params;
  const content = await readSkillContent(name);
  if (!content) {
    return { title: 'Skill not found — TWOG' };
  }
  return {
    title: `${content.title} — TWOG Proof Network`,
    description: content.description,
  };
}

export default async function SkillPage({ params }: PageProps) {
  const { name } = await params;
  const content = await readSkillContent(name);
  if (!content) notFound();

  return (
    <div className="site-shell page-shell">
      <section className="network-hero">
        <div className="network-hero-copy">
          <p className="section-kicker">
            TWOG / Proof Network / {content.is_doc ? 'Docs' : 'Skill'} / {content.slug}
          </p>
          <h1>{content.title}</h1>
          {content.description ? (
            <p className={styles.lede}>{content.description}</p>
          ) : null}
          <div className="network-hero-actions">
            <Link href="/connect" className="network-cta primary">
              Install this skill
            </Link>
            <Link href="/network" className="network-cta">
              See open packets
            </Link>
          </div>
        </div>
      </section>

      {!content.is_doc ? (
        <section className={styles.installBlock}>
          <p className="section-kicker">Install</p>
          <pre className={styles.codeBlock}>
            <code>{`# After running the TWOG installer once, symlink the bundle:
mkdir -p ~/.claude/skills/
ln -s "$(pwd)/${content.source_path.replace('/SKILL.md', '')}" ~/.claude/skills/${content.slug}

# Or just run the all-in-one installer which symlinks all 5 bundles:
twog-agent install`}</code>
          </pre>
        </section>
      ) : null}

      <section className={styles.bodySection}>
        <pre className={styles.markdownBody}>{content.body}</pre>
      </section>

      <section className={styles.metaSection}>
        <p className="section-kicker">Source</p>
        <p>
          Lives at <code className={styles.inlineCode}>{content.source_path}</code> in the
          TWOG repo. Editable; changes here change the skill Claude reads.
        </p>
        <p>
          <Link href="/connect" className="network-cta">
            Back to install
          </Link>
          {' '}
          <Link href="/network" className="network-cta">
            Open packets
          </Link>
        </p>
      </section>
    </div>
  );
}
