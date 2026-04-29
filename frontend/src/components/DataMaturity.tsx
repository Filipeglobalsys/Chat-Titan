'use client';
import { useRef, useState } from 'react';
import { Play, Loader2, RefreshCw, ShieldCheck } from 'lucide-react';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

interface DomainField {
  key: string;
  label: string;
  placeholder: string;
}

const DOMAINS: DomainField[] = [
  {
    key: 'governanca',
    label: '1. Governança de Dados',
    placeholder: 'Ex: Não existe comitê de dados formal. Políticas são definidas ad hoc por áreas. Sem data stewards designados.',
  },
  {
    key: 'arquitetura',
    label: '2. Arquitetura de Dados',
    placeholder: 'Ex: Dados distribuídos em silos por departamento. Sem arquitetura corporativa definida. Alguns sistemas legados sem integração.',
  },
  {
    key: 'modelagem',
    label: '3. Modelagem e Design de Dados',
    placeholder: 'Ex: Modelagem feita por projeto, sem padrões. Modelos não documentados. Data Warehouse parcialmente implementado.',
  },
  {
    key: 'armazenamento',
    label: '4. Armazenamento e Operações de Dados',
    placeholder: 'Ex: Bancos relacionais on-premise. Backup manual semanal. Sem estratégia de retenção definida.',
  },
  {
    key: 'seguranca',
    label: '5. Segurança de Dados',
    placeholder: 'Ex: Controle de acesso básico por perfil. Sem criptografia em trânsito. LGPD parcialmente implementada.',
  },
  {
    key: 'integracao',
    label: '6. Integração e Interoperabilidade de Dados',
    placeholder: 'Ex: Integrações via arquivos CSV agendados. Algumas APIs REST para sistemas críticos. Sem barramento corporativo.',
  },
  {
    key: 'metadados',
    label: '7. Documentação e Metadados',
    placeholder: 'Ex: Documentação desatualizada em Word/Excel. Sem catálogo de dados. Linhagem de dados não mapeada.',
  },
  {
    key: 'qualidade',
    label: '8. Qualidade de Dados',
    placeholder: 'Ex: Verificações manuais periódicas. Sem regras de qualidade automatizadas. Problemas identificados reativamente.',
  },
  {
    key: 'bi',
    label: '9. Data Warehousing e BI',
    placeholder: 'Ex: Power BI em uso por 3 áreas. DW parcial no SQL Server. Sem camada semântica consolidada.',
  },
];

export function DataMaturity() {
  const [fields, setFields] = useState<Record<string, string>>(
    Object.fromEntries(DOMAINS.map((d) => [d.key, '']))
  );
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [analysisText, setAnalysisText] = useState('');
  const [error, setError] = useState('');
  const [done, setDone] = useState(false);
  const abortRef = useRef<AbortController | null>(null);
  const bottomRef = useRef<HTMLDivElement>(null);

  function reset() {
    setAnalysisText('');
    setError('');
    setDone(false);
  }

  function handleChange(key: string, value: string) {
    setFields((prev) => ({ ...prev, [key]: value }));
  }

  async function handleAnalyze() {
    abortRef.current?.abort();
    abortRef.current = new AbortController();
    reset();
    setIsAnalyzing(true);

    try {
      const res = await fetch(`${API_URL}/api/data-maturity/analyze`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(fields),
        signal: abortRef.current.signal,
      });

      if (!res.ok || !res.body) {
        setError('Erro ao conectar com o servidor.');
        return;
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done: streamDone, value } = await reader.read();
        if (streamDone) break;
        buffer += decoder.decode(value, { stream: true });
        const parts = buffer.split('\n\n');
        buffer = parts.pop() ?? '';
        for (const part of parts) {
          const line = part.trim();
          if (!line.startsWith('data:')) continue;
          try {
            const evt = JSON.parse(line.slice(5).trim());
            if (evt.type === 'analysis_chunk') {
              setAnalysisText((p) => p + (evt.text ?? ''));
              setTimeout(() => bottomRef.current?.scrollIntoView({ behavior: 'smooth' }), 50);
            } else if (evt.type === 'done') {
              setDone(true);
            } else if (evt.type === 'error') {
              setError(evt.message ?? 'Erro desconhecido');
            }
          } catch {
            // ignore parse errors
          }
        }
      }
    } catch (e: unknown) {
      if ((e as Error).name !== 'AbortError') {
        setError('Erro de conexão com o servidor.');
      }
    } finally {
      setIsAnalyzing(false);
    }
  }

  const hasInput = Object.values(fields).some((v) => v.trim().length > 0);
  const showResults = isAnalyzing || analysisText || error;

  return (
    <div className="flex flex-col flex-1 overflow-hidden">
      {!showResults ? (
        /* ── Form ── */
        <div className="flex-1 overflow-y-auto px-6 py-6 space-y-5 scrollbar-thin">
          <div className="max-w-3xl mx-auto space-y-2">
            <p className="text-xs text-slate-400 leading-relaxed">
              Descreva o estado atual de cada domínio do DAMA-DMBOK na sua organização. Quanto mais detalhe, mais precisa será a avaliação.
            </p>
          </div>

          <div className="max-w-3xl mx-auto space-y-4">
            {DOMAINS.map((domain) => (
              <div key={domain.key} className="space-y-1.5">
                <label className="text-xs font-medium text-slate-300">{domain.label}</label>
                <textarea
                  rows={3}
                  value={fields[domain.key]}
                  onChange={(e) => handleChange(domain.key, e.target.value)}
                  placeholder={domain.placeholder}
                  className="w-full bg-powerbi-card border border-powerbi-border rounded-lg px-3 py-2 text-sm text-white placeholder:text-slate-600 resize-none focus:outline-none focus:border-powerbi-yellow/50 transition-colors"
                />
              </div>
            ))}
          </div>

          <div className="max-w-3xl mx-auto pt-2 pb-6">
            <button
              onClick={handleAnalyze}
              disabled={!hasInput || isAnalyzing}
              className="flex items-center gap-2 px-5 py-2.5 rounded-lg bg-powerbi-yellow text-black text-sm font-semibold disabled:opacity-40 disabled:cursor-not-allowed hover:bg-yellow-400 transition-colors"
            >
              <Play size={15} />
              Avaliar Maturidade
            </button>
          </div>
        </div>
      ) : (
        /* ── Results ── */
        <div className="flex-1 overflow-y-auto px-6 py-6 scrollbar-thin">
          <div className="max-w-3xl mx-auto space-y-4">
            {/* Header bar */}
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2 text-sm text-slate-300 font-medium">
                {isAnalyzing ? (
                  <>
                    <Loader2 size={15} className="text-powerbi-yellow animate-spin" />
                    Gerando avaliação DMBOK...
                  </>
                ) : done ? (
                  <>
                    <ShieldCheck size={15} className="text-green-400" />
                    Avaliação concluída
                  </>
                ) : null}
              </div>
              <button
                onClick={() => { reset(); }}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs text-slate-400 hover:text-white border border-powerbi-border hover:border-slate-500 transition-colors"
              >
                <RefreshCw size={12} />
                Nova avaliação
              </button>
            </div>

            {/* Analysis text */}
            {analysisText && (
              <div className="bg-powerbi-card border border-powerbi-border rounded-xl p-5">
                <pre className="whitespace-pre-wrap text-sm text-slate-200 leading-relaxed font-sans">
                  {analysisText}
                </pre>
              </div>
            )}

            {/* Loading skeleton */}
            {isAnalyzing && !analysisText && (
              <div className="bg-powerbi-card border border-powerbi-border rounded-xl p-5 space-y-2">
                {[1, 2, 3].map((i) => (
                  <div key={i} className="h-3 rounded bg-powerbi-border animate-pulse" style={{ width: `${70 + i * 8}%` }} />
                ))}
              </div>
            )}

            {/* Error */}
            {error && (
              <div className="bg-red-500/10 border border-red-500/30 rounded-lg px-4 py-3 text-sm text-red-400">
                {error}
              </div>
            )}

            <div ref={bottomRef} />
          </div>
        </div>
      )}
    </div>
  );
}
