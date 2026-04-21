'use client';
import { useState } from 'react';
import { ChevronDown, ChevronUp, Code2, Table2, Bot, User } from 'lucide-react';
import { ChatMessage } from '@/types';

interface Props {
  message: ChatMessage;
}

export function MessageBubble({ message }: Props) {
  const [showDax, setShowDax] = useState(false);
  const [showTable, setShowTable] = useState(false);
  const isUser = message.role === 'user';

  if (isUser) {
    return (
      <div className="flex gap-3 justify-end">
        <div className="max-w-[75%] bg-powerbi-yellow/20 border border-powerbi-yellow/30 rounded-2xl rounded-tr-sm px-4 py-3">
          <p className="text-sm text-slate-100">{message.content}</p>
        </div>
        <div className="w-8 h-8 rounded-full bg-powerbi-yellow/20 border border-powerbi-yellow/30 flex items-center justify-center flex-shrink-0">
          <User size={16} className="text-powerbi-yellow" />
        </div>
      </div>
    );
  }

  const columns = message.rows?.length
    ? Object.keys(message.rows[0])
    : [];

  return (
    <div className="flex gap-3">
      <div className="w-8 h-8 rounded-full bg-blue-500/20 border border-blue-500/30 flex items-center justify-center flex-shrink-0">
        <Bot size={16} className="text-blue-400" />
      </div>
      <div className="max-w-[85%] space-y-2">
        <div className="bg-powerbi-card border border-powerbi-border rounded-2xl rounded-tl-sm px-4 py-3">
          <p className="text-sm text-slate-200 whitespace-pre-wrap">{message.content}</p>
        </div>

        {message.daxQuery && (
          <div className="bg-powerbi-card border border-powerbi-border rounded-xl overflow-hidden">
            <button
              onClick={() => setShowDax(!showDax)}
              className="w-full flex items-center justify-between px-3 py-2 text-xs text-slate-400 hover:text-slate-200 transition-colors"
            >
              <span className="flex items-center gap-1.5">
                <Code2 size={12} />
                Query DAX gerada
              </span>
              {showDax ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
            </button>
            {showDax && (
              <pre className="px-3 pb-3 text-xs text-green-400 bg-black/30 overflow-x-auto font-mono">
                {message.daxQuery}
              </pre>
            )}
          </div>
        )}

        {message.rows && message.rows.length > 0 && (
          <div className="bg-powerbi-card border border-powerbi-border rounded-xl overflow-hidden">
            <button
              onClick={() => setShowTable(!showTable)}
              className="w-full flex items-center justify-between px-3 py-2 text-xs text-slate-400 hover:text-slate-200 transition-colors"
            >
              <span className="flex items-center gap-1.5">
                <Table2 size={12} />
                {message.rowCount} linha{message.rowCount !== 1 ? 's' : ''} retornada{message.rowCount !== 1 ? 's' : ''}
              </span>
              {showTable ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
            </button>
            {showTable && (
              <div className="overflow-x-auto max-h-64 scrollbar-thin">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-powerbi-border bg-black/20">
                      {columns.map((col) => (
                        <th key={col} className="px-3 py-2 text-left text-slate-400 font-medium whitespace-nowrap">
                          {col.replace(/^\w+\[|\]$/g, '')}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {message.rows!.slice(0, 50).map((row, i) => (
                      <tr key={i} className="border-b border-powerbi-border/50 hover:bg-white/5">
                        {columns.map((col) => (
                          <td key={col} className="px-3 py-1.5 text-slate-300 whitespace-nowrap">
                            {String(row[col] ?? '')}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
                {message.rows!.length > 50 && (
                  <p className="px-3 py-2 text-xs text-slate-500">
                    Mostrando 50 de {message.rows!.length} linhas
                  </p>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
