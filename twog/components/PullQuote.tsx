'use client';

interface PullQuoteProps {
  text: string;
}

export default function PullQuote({ text }: PullQuoteProps) {
  return (
    <blockquote className="pull-quote">
      {text}
    </blockquote>
  );
}
