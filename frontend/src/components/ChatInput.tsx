'use client';
import { Send } from 'lucide-react';
import { useState, KeyboardEvent, useRef, useEffect } from 'react';

interface Props {
  onSend: (message: string) => void;
  loading: boolean;
  disabled: boolean;
}

export function ChatInput({ onSend, loading, disabled }: Props) {
  const [value, setValue] = useState('');
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = `${Math.min(textareaRef.current.scrollHeight, 160)}px`;
    }
  }, [value]);

  function handleSend() {
    const trimmed = value.trim();
    if (!trimmed || loading || disabled) return;
    onSend(trimmed);
    setValue('');
  }

  function handleKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  }

  return (
    <div className="flex gap-3 items-end p-4 border-t border-powerbi-border bg-powerbi-dark">
      <div className="flex-1 relative">
        <textarea
          ref={textareaRef}
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={handleKeyDown}
          disabled={disabled || loading}
          placeholder={disabled ? 'Selecione um workspace e dataset para começar...' : 'Faça uma pergunta sobre seus dados...'}
          rows={1}
          className="w-full px-4 py-3 bg-powerbi-card border border-powerbi-border rounded-xl text-sm text-slate-200 placeholder-slate-500 resize-none focus:outline-none focus:border-powerbi-yellow/60 disabled:opacity-40 transition-colors"
        />
      </div>
      <button
        onClick={handleSend}
        disabled={!value.trim() || loading || disabled}
        className="w-10 h-10 rounded-xl bg-powerbi-yellow flex items-center justify-center flex-shrink-0 hover:bg-powerbi-yellow/90 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
      >
        <Send size={16} className="text-black" />
      </button>
    </div>
  );
}
