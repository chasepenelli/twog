/**
 * Read TWOG skill SKILL.md files from the repo and parse them lightly
 * for in-site rendering at /skills/[name].
 *
 * The skill bundles live one level up at <repo-root>/skills/<name>/SKILL.md.
 * We never modify them; this reader just slices off the frontmatter and
 * returns plain markdown for the page to render.
 */

import { promises as fs } from 'node:fs';
import path from 'node:path';

const REPO_ROOT = path.resolve(process.cwd(), '..');

const KNOWN_SKILLS = new Set([
  'twog-agent',
  'twog-citation-repairer',
  'twog-claim-critic',
  'twog-evidence-finder',
  'twog-validation-proposer',
]);

const KNOWN_DOCS: Record<string, { path: string; title: string }> = {
  'agent-guide': {
    path: 'docs/AGENT_PROOF_NETWORK_GUIDE.md',
    title: 'Agent Proof Network Guide',
  },
};

export function isKnownSkill(slug: string): boolean {
  return KNOWN_SKILLS.has(slug) || slug in KNOWN_DOCS;
}

export interface SkillContent {
  slug: string;
  title: string;
  description: string;
  body: string;
  source_path: string;
  is_doc: boolean;
}

interface Frontmatter {
  name?: string;
  description?: string;
}

function splitFrontmatter(raw: string): { fm: Frontmatter; body: string } {
  if (!raw.startsWith('---\n')) {
    return { fm: {}, body: raw };
  }
  const end = raw.indexOf('\n---\n', 4);
  if (end === -1) {
    return { fm: {}, body: raw };
  }
  const header = raw.slice(4, end);
  const body = raw.slice(end + 5);
  const fm: Frontmatter = {};
  for (const line of header.split('\n')) {
    const idx = line.indexOf(':');
    if (idx === -1) continue;
    const key = line.slice(0, idx).trim();
    let value = line.slice(idx + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (key === 'name' || key === 'description') fm[key] = value;
  }
  return { fm, body };
}

export async function readSkillContent(slug: string): Promise<SkillContent | null> {
  if (!isKnownSkill(slug)) return null;
  const doc = KNOWN_DOCS[slug];
  if (doc) {
    const absolute = path.join(REPO_ROOT, doc.path);
    try {
      const raw = await fs.readFile(absolute, 'utf8');
      return {
        slug,
        title: doc.title,
        description: 'Public HTTP protocol reference for agents contributing to the TWOG Proof Network.',
        body: raw,
        source_path: doc.path,
        is_doc: true,
      };
    } catch {
      return null;
    }
  }
  const sourcePath = `skills/${slug}/SKILL.md`;
  const absolute = path.join(REPO_ROOT, sourcePath);
  try {
    const raw = await fs.readFile(absolute, 'utf8');
    const { fm, body } = splitFrontmatter(raw);
    return {
      slug,
      title: fm.name ?? slug,
      description: fm.description ?? '',
      body: body.trimStart(),
      source_path: sourcePath,
      is_doc: false,
    };
  } catch {
    return null;
  }
}

export function listKnownSkills(): Array<{ slug: string; isDoc: boolean }> {
  const out: Array<{ slug: string; isDoc: boolean }> = [];
  for (const slug of KNOWN_SKILLS) out.push({ slug, isDoc: false });
  for (const slug of Object.keys(KNOWN_DOCS)) out.push({ slug, isDoc: true });
  return out.sort((a, b) => a.slug.localeCompare(b.slug));
}
