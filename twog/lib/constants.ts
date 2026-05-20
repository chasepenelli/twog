export const CONTACT_EMAIL = 'poppa@bradyandgraffiti.com';
export const CONTACT_MAILTO = `mailto:${CONTACT_EMAIL}`;

export const NAV_LINKS = [
  { href: '/candidates', label: 'Candidates' },
  { href: '/methods', label: 'Methods' },
  { href: '/architecture', label: 'Architecture' },
  { href: CONTACT_MAILTO, label: 'Contact' },
] as const;

export const DISCOVERY_TYPES: Record<string, { label: string; icon: string }> = {
  evidence: { label: 'Evidence', icon: 'EV' },
  hypothesis: { label: 'Hypothesis', icon: 'HY' },
  candidate: { label: 'Candidate', icon: 'CA' },
  validation: { label: 'Validation', icon: 'VA' },
  compute: { label: 'Compute', icon: 'CO' },
  safety: { label: 'Safety', icon: 'SA' },
};

export const TARGETS = [
  { gene: 'cKDR', aka: 'VEGFR2', fc: 2.8, role: 'angiogenesis driver' },
  { gene: 'KDR', aka: 'VEGFR2', fc: 2.4, role: 'human analog' },
  { gene: 'cFLT4', aka: 'VEGFR3', fc: 1.9, role: 'lymphatic spread' },
  { gene: 'cPDGFRA', aka: 'PDGFRA', fc: 1.7, role: 'stromal signaling' },
  { gene: 'PDGFRA', aka: 'PDGFRA', fc: 1.5, role: 'growth factor receptor' },
  { gene: 'MET', aka: 'MET', fc: 1.6, role: 'invasion pathway' },
  { gene: 'cPIK3CA', aka: 'PIK3CA', fc: 2.1, role: 'survival signaling' },
  { gene: 'PIK3CA', aka: 'PIK3CA', fc: 1.8, role: 'PI3K pathway' },
  { gene: 'AKT1', aka: 'AKT1', fc: 1.4, role: 'survival kinase' },
  { gene: 'cMTOR', aka: 'MTOR', fc: 1.6, role: 'metabolic growth' },
  { gene: 'BCL2', aka: 'BCL2', fc: 1.3, role: 'apoptosis resistance' },
  { gene: 'BRAF', aka: 'BRAF', fc: 1.5, role: 'MAPK growth' },
  { gene: 'cNRAS', aka: 'NRAS', fc: 1.4, role: 'RAS signaling' },
  { gene: 'NRAS', aka: 'NRAS', fc: 1.3, role: 'RAS analog' },
  { gene: 'MAP2K1', aka: 'MEK1', fc: 1.2, role: 'MAPK relay' },
  { gene: 'MAP2K2', aka: 'MEK2', fc: 1.2, role: 'MAPK relay' },
  { gene: 'EGFR', aka: 'EGFR', fc: 1.3, role: 'growth receptor' },
  { gene: 'cEGFR', aka: 'EGFR', fc: 1.3, role: 'canine growth receptor' },
  { gene: 'CDK4', aka: 'CDK4', fc: 1.4, role: 'cell-cycle checkpoint' },
  { gene: 'CDK6', aka: 'CDK6', fc: 1.4, role: 'cell-cycle checkpoint' },
  { gene: 'HDAC1', aka: 'HDAC1', fc: 1.3, role: 'epigenetic regulator' },
  { gene: 'HDAC2', aka: 'HDAC2', fc: 1.2, role: 'epigenetic regulator' },
  { gene: 'cTP53', aka: 'TP53', fc: 1.0, role: 'tumor suppressor context' },
  { gene: 'JAK1', aka: 'JAK1', fc: 1.2, role: 'inflammatory signaling' },
  { gene: 'JAK2', aka: 'JAK2', fc: 1.2, role: 'inflammatory signaling' },
] as const;
