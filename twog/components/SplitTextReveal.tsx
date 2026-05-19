'use client';

import { useRef } from 'react';
import { gsap } from 'gsap';
import { SplitText } from 'gsap/SplitText';
import { ScrollTrigger } from 'gsap/ScrollTrigger';
import { useGSAP } from '@gsap/react';

gsap.registerPlugin(SplitText, ScrollTrigger);

interface SplitTextRevealProps {
  children: string;
  as?: 'h1' | 'h2' | 'h3' | 'p' | 'span';
  className?: string;
  delay?: number;
  duration?: number;
  stagger?: number;
  splitType?: 'chars' | 'words' | 'lines';
  scrollTrigger?: boolean;
}

export default function SplitTextReveal({
  children,
  as: Tag = 'h1',
  className = '',
  delay = 0,
  duration = 0.8,
  stagger = 0.03,
  splitType = 'chars',
  scrollTrigger = false,
}: SplitTextRevealProps) {
  const textRef = useRef<HTMLElement>(null);

  useGSAP(() => {
    const el = textRef.current;
    if (!el) return;

    SplitText.create(el, {
      type: splitType,
      mask: 'lines',
      autoSplit: true,
      onSplit(self) {
        const targets =
          splitType === 'chars' ? self.chars :
          splitType === 'words' ? self.words :
          self.lines;

        const anim = gsap.from(targets, {
          y: '100%',
          opacity: 0,
          duration,
          delay,
          stagger,
          ease: 'power4.out',
          ...(scrollTrigger
            ? {
                scrollTrigger: {
                  trigger: el,
                  start: 'top 85%',
                  toggleActions: 'play none none reverse',
                },
              }
            : {}),
        });

        return anim;
      },
    });
  }, { scope: textRef });

  return (
    <Tag
      ref={textRef as React.Ref<HTMLHeadingElement>}
      className={`split-text-target ${className}`}
    >
      {children}
    </Tag>
  );
}
