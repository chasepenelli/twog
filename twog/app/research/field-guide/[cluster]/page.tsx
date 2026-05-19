import type { Metadata } from 'next';
import FieldGuideDetail from '@/components/FieldGuideDetail';

export const metadata: Metadata = {
  title: 'Field Guide — TWOG',
  description:
    'A single research neighborhood: what we\u2019ve read, which three papers to start with, and the gap we still can\u2019t see into.',
};

interface Props {
  params: Promise<{ cluster: string }>;
}

export default async function FieldGuideDetailPage({ params }: Props) {
  const { cluster } = await params;
  return <FieldGuideDetail clusterParam={cluster} />;
}
