"""Parse genomics-paper supplementary tables into analysis-ready cohorts.

Built for the recurring need to turn a supplementary PDF (per-sample genotypes + RNA-seq archive
maps) into a stratified crux cohort — first instance: Megquier 2019 (PMC7067513) Tables S4+S5.
The pure parsing logic here is dependency-free and unit-tested; PDF text extraction (pdfplumber)
is isolated in ``read_pdf_pages`` so this module imports without heavy deps.
"""

from __future__ import annotations

import re

# Significantly-mutated genes examined for RNA-seq validation in Megquier Table S5.
MEGQUIER_EXAMINED_GENES = ("ARPC1A", "ORC1", "PIK3CA", "PIK3R1", "RASA1", "TP53")

_VARIANT = re.compile(r"chr[\w]+:\d+:([A-Z0-9]+):")  # gene = 3rd field of chrX:pos:GENE:ref:alt
_HSA = re.compile(r"\bHSA_(\d+)\b")
_RNASEQ_ID = re.compile(r"^((?:DHSA|CHAD)-\S+)")  # Table S4 col 1 = expression-matrix column header
_GSM = re.compile(r"\b(GSM\d+)\b")
_SAMN = re.compile(r"\b(SAMN\d+)\b")


def parse_s5_genotypes(s5_text: str, examined_genes=MEGQUIER_EXAMINED_GENES) -> dict[str, set[str]]:
    """HSA_x -> set of examined genes mutated (Table S5). Absence of a gene for an exome∩RNA-seq
    individual ⇒ wild-type for that gene (S5 lists every examined-gene mutation for such samples)."""
    geno: dict[str, set[str]] = {}
    cur: str | None = None
    for line in s5_text.splitlines():
        m = _HSA.search(line)
        if m:
            cur = f"HSA_{m.group(1)}"
        gv = _VARIANT.search(line)
        if gv and cur and gv.group(1) in examined_genes:
            geno.setdefault(cur, set()).add(gv.group(1))
    return geno


def parse_s4_map(s4_texts: list[str]) -> dict[str, list[dict]]:
    """HSA_x -> list of RNA-seq rows (Table S4), each with rnaseq_id / gsm / biosample. A GSM means
    the sample is in a public expression matrix (GSE95183); biosample-only = raw SRA."""
    rows: dict[str, list[dict]] = {}
    for text in s4_texts:
        for line in text.splitlines():
            hm = _HSA.search(line)
            if not hm:
                continue
            rid = _RNASEQ_ID.match(line.strip())
            gsm = _GSM.search(line)
            samn = _SAMN.search(line)
            rows.setdefault(f"HSA_{hm.group(1)}", []).append({
                "rnaseq_id": rid.group(1) if rid else None,
                "gsm": gsm.group(1) if gsm else None,
                "biosample": samn.group(1) if samn else None,
            })
    return rows


def build_cohort(s4_texts: list[str], s5_text: str, *, stratify_gene: str = "PIK3CA") -> dict:
    """Join S4 (archive map) + S5 (genotypes) into a cohort + a public-expression crux stratum
    keyed by RNA-seq ID (= expression-matrix column). Pure; testable without a PDF."""
    geno = parse_s5_genotypes(s5_text)
    s4 = parse_s4_map(s4_texts)
    cohort: dict[str, dict] = {}
    for hsa, rnaseq_rows in s4.items():
        genes = geno.get(hsa, set())
        public_ids = [r["rnaseq_id"] for r in rnaseq_rows if r["gsm"] and r["rnaseq_id"]]
        cohort[hsa] = {
            "genotype": {g: ("mutant" if g in genes else "wt") for g in MEGQUIER_EXAMINED_GENES},
            "mutated_examined_genes": sorted(genes),
            "has_public_expression": bool(public_ids),
            "rnaseq_ids_public": public_ids,
            "biosamples": [r["biosample"] for r in rnaseq_rows if r["biosample"]],
        }
    strata: dict[str, str] = {}
    for c in cohort.values():
        for rid in c["rnaseq_ids_public"]:
            strata[rid] = c["genotype"][stratify_gene]
    return {
        "cohort": cohort,
        "crux_strata_public": strata,
        "counts": {
            "mutant_public": sum(v == "mutant" for v in strata.values()),
            "wt_public": sum(v == "wt" for v in strata.values()),
            "total_exome_matched_rnaseq": len(cohort),
        },
    }


def read_pdf_pages(pdf_path: str) -> list[str]:
    """Extract per-page text from a PDF (lazy pdfplumber import so the module stays light)."""
    import pdfplumber

    with pdfplumber.open(pdf_path) as pdf:
        return [(pg.extract_text() or "") for pg in pdf.pages]
