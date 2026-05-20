export interface ThesisSection {
  number: number;
  slug: string;
  title: string;
  summary: string;
  content?: string;
  rawMarkdown?: string;
  pullQuote?: string;
}

export interface ParsedThesis {
  headline: string;
  dateRange?: string;
  stats: {
    papers: number;
    molecules: number;
    cycles: number;
  };
  confidence?: {
    score: number;
    label?: string;
  };
  sections: ThesisSection[];
}

export function parseThesis(markdown: string): ParsedThesis {
  const lines = markdown.split(/\r?\n/);
  const headline = lines.find((line) => line.trim().startsWith('# '))?.replace(/^#\s+/, '').trim() || 'TWOG Review';

  return {
    headline,
    stats: {
      papers: 0,
      molecules: 0,
      cycles: 0,
    },
    sections: [],
  };
}
