'use client';

interface SectionDividerProps {
  number?: number;
}

export default function SectionDivider({ number }: SectionDividerProps) {
  return (
    <div className="section-divider">
      {number ? `--- ${String(number).padStart(2, '0')} ---` : '* * *'}
    </div>
  );
}
