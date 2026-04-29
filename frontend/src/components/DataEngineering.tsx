'use client';
import { useEffect, useRef, useState } from 'react';
import {
  Database, Play, CheckCircle, XCircle, Loader2,
  ChevronRight, Eye, EyeOff, ArrowRightLeft, RefreshCw,
} from 'lucide-react';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

type Platform = 'databricks' | 'fabric';
type Mode = 'analysis' | 'ingestion';

// ── Types ─────────────────────────────────────────────────────────────────────

interface ProgressStep {
  key: string;
  message: string;
  status: 'running' | 'done' | 'error';
}

interface SseEvent {
  type: string;
  step?: string;
  message?: string;
  text?: string;
  table?: string;
  rows?: number;
  columns?: string[];
}

interface WorkspaceOption {
  workspace_id: string;
  workspace_name: string;
  lakehouses: { id: string; name: string }[];
}

// ── Shared streaming hook ─────────────────────────────────────────────────────

function useAnalysis() {
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [steps, setSteps] = useState<ProgressStep[]>([]);
  const [analysisText, setAnalysisText] = useState('');
  const [analysisStarted, setAnalysisStarted] = useState(false);
  const [error, setError] = useState('');
  const [done, setDone] = useState(false);
  const [donePayload, setDonePayload] = useState<SseEvent | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  function reset() {
    setSteps([]);
    setAnalysisText('');
    setAnalysisStarted(false);
    setError('');
    setDone(false);
    setDonePayload(null);
  }

  function upsertStep(key: string, message: string, status: ProgressStep['status']) {
    setSteps((prev) => {
      const idx = prev.findIndex((s) => s.key === key);
      const item = { key, message, status };
      if (idx >= 0) { const n = [...prev]; n[idx] = item; return n; }
      return [...prev, item];
    });
  }

  function handleEvent(evt: SseEvent) {
    switch (evt.type) {
      case 'progress':
        upsertStep(evt.step ?? 'step', evt.message ?? '', evt.message?.startsWith('✓') ? 'done' : 'running');
        break;
      case 'analysis_start': setAnalysisStarted(true); break;
      case 'analysis_chunk': setAnalysisText((p) => p + (evt.text ?? '')); break;
      case 'done':
        setDone(true);
        setDonePayload(evt);
        setSteps((p) => p.map((s) => s.status === 'running' ? { ...s, status: 'done' } : s));
        break;
      case 'error':
        setError(evt.message ?? 'Erro desconhecido');
        setSteps((p) => p.map((s) => s.status === 'running' ? { ...s, status: 'error' } : s));
        break;
    }
  }

  async function run(endpoint: string, body: Record<string, unknown>) {
    abortRef.current?.abort();
    abortRef.current = new AbortController();
    reset();
    setIsAnalyzing(true);
    try {
      const res = await fetch(`${API_URL}${endpoint}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        signal: abortRef.current.signal,
      });
      if (!res.ok || !res.body) {
        setError('Erro ao conectar com o servidor.');
        return;
      }
      const reader = res.body.getReader();
      const dec = new TextDecoder();
      let buf = '';
      while (true) {
        const { done: sd, value } = await reader.read();
        if (sd) break;
        buf += dec.decode(value, { stream: true });
        const lines = buf.split('\n');
        buf = lines.pop() ?? '';
        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          try { handleEvent(JSON.parse(line.slice(6))); } catch { /* ignore */ }
        }
      }
    } catch (err: unknown) {
      if (err instanceof Error && err.name !== 'AbortError') setError(`Erro: ${err.message}`);
    } finally {
      setIsAnalyzing(false);
    }
  }

  function stop() { abortRef.current?.abort(); setIsAnalyzing(false); }

  return { isAnalyzing, steps, analysisText, analysisStarted, error, done, donePayload, reset, run, stop };
}

// ── Shared UI atoms ───────────────────────────────────────────────────────────

function PasswordInput({ value, onChange, placeholder, disabled }: {
  value: string; onChange: (v: string) => void; placeholder?: string; disabled?: boolean;
}) {
  const [show, setShow] = useState(false);
  return (
    <div className="relative">
      <input
        type={show ? 'text' : 'password'}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        disabled={disabled}
        className="w-full bg-powerbi-dark border border-powerbi-border rounded-lg px-3 py-2 pr-10 text-sm text-white placeholder-slate-600 focus:outline-none focus:border-powerbi-yellow/50 disabled:opacity-50"
      />
      <button type="button" onClick={() => setShow((v) => !v)} className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-300">
        {show ? <EyeOff size={15} /> : <Eye size={15} />}
      </button>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="block text-xs text-slate-500 mb-1">{label}</label>
      {children}
    </div>
  );
}

function TextInput({ value, onChange, placeholder, disabled, type = 'text' }: {
  value: string; onChange: (v: string) => void; placeholder?: string; disabled?: boolean; type?: string;
}) {
  return (
    <input
      type={type}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      disabled={disabled}
      className="w-full bg-powerbi-dark border border-powerbi-border rounded-lg px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:border-powerbi-yellow/50 disabled:opacity-50"
    />
  );
}

function RunButtons({ canRun, isRunning, done, onRun, onStop, onReset, label = 'Analisar Ambiente' }: {
  canRun: boolean; isRunning: boolean; done: boolean;
  onRun: () => void; onStop: () => void; onReset: () => void; label?: string;
}) {
  return (
    <div className="flex gap-2 pt-1">
      <button onClick={onRun} disabled={!canRun}
        className="flex items-center gap-2 px-4 py-2 bg-powerbi-yellow text-black text-sm font-semibold rounded-lg hover:bg-yellow-300 disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
        {isRunning ? <Loader2 size={15} className="animate-spin" /> : <Play size={15} />}
        {isRunning ? 'Executando...' : label}
      </button>
      {isRunning && (
        <button onClick={onStop} className="px-4 py-2 border border-red-500/40 text-red-400 text-sm rounded-lg hover:bg-red-500/10 transition-colors">
          Parar
        </button>
      )}
      {done && !isRunning && (
        <button onClick={onReset} className="px-4 py-2 border border-powerbi-border text-slate-400 text-sm rounded-lg hover:bg-powerbi-card transition-colors">
          Novo
        </button>
      )}
    </div>
  );
}

function ProgressPanel({ steps, error, isRunning, done, analysisText, analysisStarted, donePayload, onReset }: {
  steps: ProgressStep[]; error: string; isRunning: boolean; done: boolean;
  analysisText: string; analysisStarted: boolean; donePayload: SseEvent | null; onReset: () => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => { if (ref.current) ref.current.scrollTop = ref.current.scrollHeight; }, [analysisText]);

  return (
    <div className="space-y-4">
      {steps.length > 0 && (
        <div className="bg-powerbi-card border border-powerbi-border rounded-xl p-4 space-y-2">
          <p className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-3">Progresso</p>
          {steps.map((s) => (
            <div key={s.key} className="flex items-center gap-2 text-sm">
              {s.status === 'running' && <Loader2 size={14} className="text-powerbi-yellow animate-spin shrink-0" />}
              {s.status === 'done' && <CheckCircle size={14} className="text-green-400 shrink-0" />}
              {s.status === 'error' && <XCircle size={14} className="text-red-400 shrink-0" />}
              <span className={s.status === 'error' ? 'text-red-400' : s.status === 'done' ? 'text-slate-300' : 'text-slate-400'}>{s.message}</span>
            </div>
          ))}
        </div>
      )}

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl p-4 text-sm text-red-400 flex items-start gap-2">
          <XCircle size={15} className="shrink-0 mt-0.5" /> {error}
        </div>
      )}

      {/* Ingestion done summary */}
      {done && donePayload?.table && (
        <div className="bg-green-500/10 border border-green-500/30 rounded-xl p-4 space-y-1">
          <p className="text-sm font-semibold text-green-400 flex items-center gap-2">
            <CheckCircle size={15} /> Ingestão concluída
          </p>
          <p className="text-sm text-slate-300">
            Tabela <span className="font-mono text-white">{donePayload.table}</span> criada com{' '}
            <span className="text-white">{(donePayload.rows ?? 0).toLocaleString()}</span> linhas e{' '}
            <span className="text-white">{donePayload.columns?.length ?? 0}</span> colunas.
          </p>
          {donePayload.columns && donePayload.columns.length > 0 && (
            <p className="text-xs text-slate-500 font-mono">{donePayload.columns.slice(0, 12).join(', ')}{donePayload.columns.length > 12 ? '...' : ''}</p>
          )}
          <button onClick={onReset} className="mt-2 text-xs text-slate-400 hover:text-slate-200 underline">
            Nova ingestão
          </button>
        </div>
      )}

      {/* Analysis streaming terminal */}
      {analysisStarted && (
        <div className="bg-[#0d1117] border border-powerbi-border rounded-xl overflow-hidden">
          <div className="flex items-center justify-between px-4 py-2 border-b border-powerbi-border bg-powerbi-card/60">
            <div className="flex items-center gap-2">
              <ChevronRight size={14} className="text-powerbi-yellow" />
              <span className="text-xs font-medium text-slate-400">Análise do Agente</span>
            </div>
            <div className="flex items-center gap-3">
              {isRunning && <span className="text-xs text-slate-500 flex items-center gap-1"><Loader2 size={11} className="animate-spin" /> gerando...</span>}
              {done && (
                <>
                  <span className="text-xs text-green-400 flex items-center gap-1"><CheckCircle size={11} /> concluído</span>
                  <button onClick={onReset} className="text-xs text-slate-500 hover:text-slate-300">Nova análise</button>
                </>
              )}
            </div>
          </div>
          <div ref={ref} className="p-5 font-mono text-sm text-slate-300 leading-relaxed whitespace-pre-wrap overflow-y-auto max-h-[calc(100vh-460px)] min-h-[200px]">
            {analysisText}
            {isRunning && <span className="inline-block w-2 h-4 bg-powerbi-yellow ml-0.5 animate-pulse align-text-bottom" />}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Analysis panel ────────────────────────────────────────────────────────────

function FabricCredFields({ tenantId, setTenantId, clientId, setClientId, clientSecret, setClientSecret, disabled }: {
  tenantId: string; setTenantId: (v: string) => void;
  clientId: string; setClientId: (v: string) => void;
  clientSecret: string; setClientSecret: (v: string) => void;
  disabled?: boolean;
}) {
  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
      <Field label="Tenant ID">
        <TextInput value={tenantId} onChange={setTenantId} placeholder="xxxxxxxx-xxxx-xxxx-xxxx" disabled={disabled} />
      </Field>
      <Field label="Client ID (App Registration)">
        <TextInput value={clientId} onChange={setClientId} placeholder="xxxxxxxx-xxxx-xxxx-xxxx" disabled={disabled} />
      </Field>
      <Field label="Client Secret">
        <PasswordInput value={clientSecret} onChange={setClientSecret} placeholder="••••••••••••••••" disabled={disabled} />
      </Field>
    </div>
  );
}

function AnalysisPanel() {
  const [platform, setPlatform] = useState<Platform>('databricks');
  const analysis = useAnalysis();

  // Databricks fields
  const [dbHost, setDbHost] = useState('');
  const [dbToken, setDbToken] = useState('');

  // Fabric fields
  const [tenantId, setTenantId] = useState('');
  const [clientId, setClientId] = useState('');
  const [clientSecret, setClientSecret] = useState('');

  function switchPlatform(p: Platform) {
    if (p === platform) return;
    analysis.reset(); analysis.stop(); setPlatform(p);
  }

  const canRun = platform === 'databricks'
    ? dbHost.trim() !== '' && dbToken.trim() !== '' && !analysis.isAnalyzing
    : tenantId.trim() !== '' && clientId.trim() !== '' && clientSecret.trim() !== '' && !analysis.isAnalyzing;

  function handleRun() {
    if (platform === 'databricks') {
      analysis.run('/api/databricks/analyze', { host: dbHost.trim(), token: dbToken.trim() });
    } else {
      analysis.run('/api/fabric/analyze', { tenant_id: tenantId.trim(), client_id: clientId.trim(), client_secret: clientSecret.trim() });
    }
  }

  const isEmpty = !analysis.isAnalyzing && analysis.steps.length === 0 && !analysis.error;

  return (
    <>
      <div className="px-6 py-5 border-b border-powerbi-border bg-powerbi-card/30 space-y-4">
        <div className="flex items-center gap-3">
          <span className="text-xs text-slate-500 uppercase tracking-wider">Plataforma</span>
          <div className="flex items-center bg-powerbi-dark border border-powerbi-border rounded-lg p-0.5 gap-0.5">
            {(['databricks', 'fabric'] as Platform[]).map((p) => (
              <button key={p} onClick={() => switchPlatform(p)}
                className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${platform === p ? 'bg-powerbi-yellow text-black' : 'text-slate-400 hover:text-white'}`}>
                {p === 'databricks' ? 'Databricks' : 'Microsoft Fabric'}
              </button>
            ))}
          </div>
        </div>

        {platform === 'databricks' ? (
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <Field label="Workspace URL">
              <TextInput value={dbHost} onChange={setDbHost} placeholder="https://adb-xxx.azuredatabricks.net" disabled={analysis.isAnalyzing} />
            </Field>
            <Field label="Personal Access Token">
              <PasswordInput value={dbToken} onChange={setDbToken} placeholder="dapi••••••••••••••••" disabled={analysis.isAnalyzing} />
            </Field>
          </div>
        ) : (
          <FabricCredFields tenantId={tenantId} setTenantId={setTenantId} clientId={clientId} setClientId={setClientId} clientSecret={clientSecret} setClientSecret={setClientSecret} disabled={analysis.isAnalyzing} />
        )}

        <RunButtons canRun={canRun} isRunning={analysis.isAnalyzing} done={analysis.done} onRun={handleRun} onStop={analysis.stop} onReset={analysis.reset} />
      </div>

      <div className="flex-1 overflow-y-auto px-6 py-4 scrollbar-thin">
        {isEmpty ? (
          <div className="flex flex-col items-center justify-center py-20 text-center">
            <div className="w-14 h-14 rounded-2xl bg-powerbi-card border border-powerbi-border flex items-center justify-center mb-4">
              <Database size={24} className="text-slate-600" />
            </div>
            <p className="text-slate-500 text-sm">{platform === 'databricks' ? 'Informe o workspace URL e o Personal Access Token.' : 'Informe as credenciais do Service Principal.'}</p>
            <p className="text-slate-600 text-xs mt-1">{platform === 'databricks' ? 'O agente inspeciona clusters, jobs, Unity Catalog e tabelas.' : 'O agente inspeciona workspaces, Lakehouses, Warehouses e Pipelines.'}</p>
          </div>
        ) : (
          <ProgressPanel {...analysis} onReset={analysis.reset} />
        )}
      </div>
    </>
  );
}

// ── Ingestion panel ───────────────────────────────────────────────────────────

const DB_DIALECTS = [
  { value: 'mssql', label: 'SQL Server' },
  { value: 'postgresql', label: 'PostgreSQL' },
  { value: 'mysql', label: 'MySQL' },
];

function IngestionPanel() {
  // Source
  const [dialect, setDialect] = useState('mssql');
  const [host, setHost] = useState('');
  const [port, setPort] = useState('');
  const [dbName, setDbName] = useState('');
  const [dbUser, setDbUser] = useState('');
  const [dbPassword, setDbPassword] = useState('');
  const [sql, setSql] = useState('');

  // Fabric creds
  const [tenantId, setTenantId] = useState('');
  const [clientId, setClientId] = useState('');
  const [clientSecret, setClientSecret] = useState('');

  // Workspace/Lakehouse picker
  const [loadingWs, setLoadingWs] = useState(false);
  const [wsOptions, setWsOptions] = useState<WorkspaceOption[]>([]);
  const [wsError, setWsError] = useState('');
  const [selectedWs, setSelectedWs] = useState('');
  const [selectedLh, setSelectedLh] = useState('');
  const [tableName, setTableName] = useState('');

  const ingestion = useAnalysis();

  const lakehouses = wsOptions.find((w) => w.workspace_id === selectedWs)?.lakehouses ?? [];

  async function loadWorkspaces() {
    if (!tenantId.trim() || !clientId.trim() || !clientSecret.trim()) return;
    setLoadingWs(true);
    setWsError('');
    setWsOptions([]);
    setSelectedWs('');
    setSelectedLh('');
    try {
      const res = await fetch(`${API_URL}/api/fabric/lakehouses`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tenant_id: tenantId.trim(), client_id: clientId.trim(), client_secret: clientSecret.trim() }),
      });
      if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.detail || 'Erro ao carregar'); }
      const data: WorkspaceOption[] = await res.json();
      if (data.length === 0) { setWsError('Nenhum workspace com Lakehouse encontrado.'); return; }
      setWsOptions(data);
    } catch (e: unknown) {
      setWsError(e instanceof Error ? e.message : 'Erro desconhecido');
    } finally {
      setLoadingWs(false);
    }
  }

  const canRun =
    host.trim() !== '' && dbName.trim() !== '' && dbUser.trim() !== '' &&
    dbPassword.trim() !== '' && sql.trim() !== '' &&
    tenantId.trim() !== '' && clientId.trim() !== '' && clientSecret.trim() !== '' &&
    selectedWs !== '' && selectedLh !== '' && tableName.trim() !== '' && !ingestion.isAnalyzing;

  function handleRun() {
    ingestion.run('/api/ingestion/run', {
      db_dialect: dialect,
      db_host: host.trim(),
      db_port: port.trim() ? Number(port.trim()) : null,
      db_name: dbName.trim(),
      db_user: dbUser.trim(),
      db_password: dbPassword.trim(),
      sql_query: sql.trim(),
      tenant_id: tenantId.trim(),
      client_id: clientId.trim(),
      client_secret: clientSecret.trim(),
      workspace_id: selectedWs,
      lakehouse_id: selectedLh,
      table_name: tableName.trim(),
    });
  }

  const selectCls = "w-full bg-powerbi-dark border border-powerbi-border rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-powerbi-yellow/50 disabled:opacity-50";
  const sectionTitle = (t: string) => <p className="text-xs font-semibold text-slate-400 uppercase tracking-wider">{t}</p>;

  const isEmpty = !ingestion.isAnalyzing && ingestion.steps.length === 0 && !ingestion.error;

  return (
    <>
      <div className="px-6 py-5 border-b border-powerbi-border bg-powerbi-card/30 space-y-5 overflow-y-auto max-h-[55vh] scrollbar-thin">

        {/* Source DB */}
        <div className="space-y-3">
          {sectionTitle('1. Fonte de dados')}
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <Field label="Banco">
              <select value={dialect} onChange={(e) => setDialect(e.target.value)} className={selectCls}>
                {DB_DIALECTS.map((d) => <option key={d.value} value={d.value}>{d.label}</option>)}
              </select>
            </Field>
            <Field label="Host / IP">
              <TextInput value={host} onChange={setHost} placeholder="servidor.domain.com" disabled={ingestion.isAnalyzing} />
            </Field>
            <Field label="Porta (opcional)">
              <TextInput value={port} onChange={setPort} placeholder="1433" disabled={ingestion.isAnalyzing} type="number" />
            </Field>
            <Field label="Database">
              <TextInput value={dbName} onChange={setDbName} placeholder="nome_do_banco" disabled={ingestion.isAnalyzing} />
            </Field>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <Field label="Usuário">
              <TextInput value={dbUser} onChange={setDbUser} placeholder="sa" disabled={ingestion.isAnalyzing} />
            </Field>
            <Field label="Senha">
              <PasswordInput value={dbPassword} onChange={setDbPassword} placeholder="••••••••" disabled={ingestion.isAnalyzing} />
            </Field>
          </div>
        </div>

        {/* SQL Query */}
        <div className="space-y-2">
          {sectionTitle('2. Query SQL')}
          <textarea
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            disabled={ingestion.isAnalyzing}
            placeholder={'SELECT\n  id,\n  nome,\n  valor,\n  data_criacao\nFROM dbo.vendas\nWHERE data_criacao >= \'2024-01-01\''}
            rows={6}
            className="w-full bg-powerbi-dark border border-powerbi-border rounded-lg px-3 py-2 text-sm text-white font-mono placeholder-slate-700 focus:outline-none focus:border-powerbi-yellow/50 disabled:opacity-50 resize-y"
          />
        </div>

        {/* Fabric Target */}
        <div className="space-y-3">
          {sectionTitle('3. Destino — Microsoft Fabric')}
          <FabricCredFields
            tenantId={tenantId} setTenantId={setTenantId}
            clientId={clientId} setClientId={setClientId}
            clientSecret={clientSecret} setClientSecret={setClientSecret}
            disabled={ingestion.isAnalyzing}
          />
          <div className="flex items-end gap-3">
            <div className="flex-1">
              <Field label="Workspace">
                <select value={selectedWs} onChange={(e) => { setSelectedWs(e.target.value); setSelectedLh(''); }}
                  disabled={wsOptions.length === 0 || ingestion.isAnalyzing} className={selectCls}>
                  <option value="">— selecione —</option>
                  {wsOptions.map((w) => <option key={w.workspace_id} value={w.workspace_id}>{w.workspace_name}</option>)}
                </select>
              </Field>
            </div>
            <div className="flex-1">
              <Field label="Lakehouse">
                <select value={selectedLh} onChange={(e) => setSelectedLh(e.target.value)}
                  disabled={lakehouses.length === 0 || ingestion.isAnalyzing} className={selectCls}>
                  <option value="">— selecione —</option>
                  {lakehouses.map((lh) => <option key={lh.id} value={lh.id}>{lh.name}</option>)}
                </select>
              </Field>
            </div>
            <button onClick={loadWorkspaces} disabled={!tenantId || !clientId || !clientSecret || loadingWs || ingestion.isAnalyzing}
              title="Carregar workspaces"
              className="mb-0 flex items-center gap-1.5 px-3 py-2 border border-powerbi-border text-slate-400 text-xs rounded-lg hover:bg-powerbi-card disabled:opacity-40 transition-colors">
              {loadingWs ? <Loader2 size={13} className="animate-spin" /> : <RefreshCw size={13} />}
              Carregar
            </button>
          </div>
          {wsError && <p className="text-xs text-red-400">{wsError}</p>}
          <Field label="Nome da tabela destino">
            <TextInput value={tableName} onChange={setTableName} placeholder="vendas_2024" disabled={ingestion.isAnalyzing} />
          </Field>
        </div>

        <RunButtons canRun={canRun} isRunning={ingestion.isAnalyzing} done={ingestion.done}
          onRun={handleRun} onStop={ingestion.stop} onReset={ingestion.reset} label="Executar Ingestão" />
      </div>

      <div className="flex-1 overflow-y-auto px-6 py-4 scrollbar-thin">
        {isEmpty ? (
          <div className="flex flex-col items-center justify-center py-16 text-center">
            <div className="w-14 h-14 rounded-2xl bg-powerbi-card border border-powerbi-border flex items-center justify-center mb-4">
              <ArrowRightLeft size={24} className="text-slate-600" />
            </div>
            <p className="text-slate-500 text-sm">Preencha a fonte, a query SQL e o destino no Fabric.</p>
            <p className="text-slate-600 text-xs mt-1">Os dados serão carregados como tabela Delta no Lakehouse selecionado.</p>
          </div>
        ) : (
          <ProgressPanel {...ingestion} onReset={ingestion.reset} />
        )}
      </div>
    </>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export function DataEngineering() {
  const [mode, setMode] = useState<Mode>('analysis');

  return (
    <div className="flex flex-col h-full overflow-hidden">
      {/* Mode tabs */}
      <div className="px-6 pt-4 pb-0 border-b border-powerbi-border bg-powerbi-card/20">
        <div className="flex gap-1">
          {([['analysis', 'Análise de Ambiente'], ['ingestion', 'Ingestão de Dados']] as [Mode, string][]).map(([m, label]) => (
            <button key={m} onClick={() => setMode(m)}
              className={`px-4 py-2 text-sm font-medium rounded-t-lg border-b-2 transition-colors ${
                mode === m
                  ? 'text-white border-powerbi-yellow bg-powerbi-card/40'
                  : 'text-slate-500 border-transparent hover:text-slate-300'
              }`}>
              {label}
            </button>
          ))}
        </div>
      </div>

      <div className="flex-1 overflow-hidden flex flex-col">
        {mode === 'analysis' ? <AnalysisPanel /> : <IngestionPanel />}
      </div>
    </div>
  );
}
