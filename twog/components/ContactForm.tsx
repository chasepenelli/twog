'use client';

import { FormEvent, useMemo, useState } from 'react';
import { CONTACT_EMAIL } from '@/lib/constants';

const TOPICS = [
  'Candidate review',
  'Research collaboration',
  'Compute / infrastructure',
  'Press / Substack',
  'Other',
] as const;

export function ContactForm() {
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [topic, setTopic] = useState<(typeof TOPICS)[number]>('Research collaboration');
  const [message, setMessage] = useState('');
  const [status, setStatus] = useState('');

  const mailtoHref = useMemo(() => {
    const subject = `TWOG contact: ${topic}`;
    const body = [
      `Name: ${name || ''}`,
      `Email: ${email || ''}`,
      `Topic: ${topic}`,
      '',
      message || '',
    ].join('\n');

    return `mailto:${CONTACT_EMAIL}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
  }, [email, message, name, topic]);

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!message.trim()) {
      setStatus('Add a short note first.');
      return;
    }

    setStatus('Opening your email draft...');
    window.location.href = mailtoHref;
  }

  return (
    <form className="contact-form" onSubmit={handleSubmit}>
      <div className="contact-form-head">
        <span>Direct signal</span>
        <p>Send a note about the project, a candidate, or a collaboration path.</p>
      </div>

      <label>
        <span>Name</span>
        <input
          autoComplete="name"
          name="name"
          onChange={(event) => setName(event.target.value)}
          placeholder="Your name"
          type="text"
          value={name}
        />
      </label>

      <label>
        <span>Email</span>
        <input
          autoComplete="email"
          name="email"
          onChange={(event) => setEmail(event.target.value)}
          placeholder="you@example.com"
          type="email"
          value={email}
        />
      </label>

      <label>
        <span>Topic</span>
        <select name="topic" onChange={(event) => setTopic(event.target.value as (typeof TOPICS)[number])} value={topic}>
          {TOPICS.map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
      </label>

      <label>
        <span>Note</span>
        <textarea
          name="message"
          onChange={(event) => setMessage(event.target.value)}
          placeholder="What should I know?"
          rows={4}
          value={message}
        />
      </label>

      <button type="submit">Open email draft</button>
      <a className="contact-fallback" href={mailtoHref}>
        Or email {CONTACT_EMAIL}
      </a>
      {status && <p className="contact-status">{status}</p>}
    </form>
  );
}
