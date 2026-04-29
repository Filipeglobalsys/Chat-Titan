'use client';
import { useEffect, useRef, useState } from 'react';
import { BarChart3, Database, Loader2, ShieldCheck } from 'lucide-react';
import { api } from '@/lib/api';
import { Workspace, Dataset, ChatMessage, AppTab } from '@/types';
import { WorkspaceSelector, DatasetSelector } from '@/components/Selectors';
import { MessageBubble } from '@/components/MessageBubble';
import { ChatInput } from '@/components/ChatInput';
import { SyncButton } from '@/components/SyncButton';
import { DataEngineering } from '@/components/DataEngineering';
import { DataMaturity } from '@/components/DataMaturity';

const WELCOME: ChatMessage = {
  id: 'welcome',
  role: 'assistant',
  content:
    'Olá! Sou o Power BI Copilot. Selecione um workspace e dataset, depois faça qualquer pergunta sobre seus dados em linguagem natural.\n\nExemplos:\n• "Qual foi o total de vendas no último trimestre?"\n• "Quais são os 10 produtos mais vendidos?"\n• "Mostre a receita mensal por região"',
  timestamp: new Date(),
};

export default function Home() {
  const [activeTab, setActiveTab] = useState<AppTab>('powerbi');
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [selectedWorkspace, setSelectedWorkspace] = useState('');
  const [selectedDataset, setSelectedDataset] = useState('');
  const [messages, setMessages] = useState<ChatMessage[]>([WELCOME]);
  const [loading, setLoading] = useState(false);
  const [loadingWorkspaces, setLoadingWorkspaces] = useState(true);
  const [loadingDatasets, setLoadingDatasets] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    loadWorkspaces();
  }, []);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  async function loadWorkspaces() {
    setLoadingWorkspaces(true);
    try {
      const data = await api.getWorkspaces();
      setWorkspaces(data);
    } catch {
    } finally {
      setLoadingWorkspaces(false);
    }
  }

  async function handleWorkspaceChange(id: string) {
    setSelectedWorkspace(id);
    setSelectedDataset('');
    setDatasets([]);
    if (!id) return;
    setLoadingDatasets(true);
    try {
      const data = await api.getDatasets(id);
      setDatasets(data);
    } catch {
    } finally {
      setLoadingDatasets(false);
    }
  }

  async function handleSend(question: string) {
    const userMsg: ChatMessage = {
      id: Date.now().toString(),
      role: 'user',
      content: question,
      timestamp: new Date(),
    };
    setMessages((prev) => [...prev, userMsg]);
    setLoading(true);

    try {
      const result = await api.chat(question, selectedDataset);
      const assistantMsg: ChatMessage = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: result.answer,
        daxQuery: result.daxQuery,
        rows: result.rows,
        rowCount: result.rowCount,
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, assistantMsg]);
    } catch (err: unknown) {
      const errMsg: ChatMessage = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: `Erro: ${err instanceof Error ? err.message : 'Ocorreu um erro inesperado.'}`,
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errMsg]);
    } finally {
      setLoading(false);
    }
  }

  const selectedDatasetName = datasets.find((d) => d.id === selectedDataset)?.name;

  return (
    <div className="flex flex-col h-screen bg-powerbi-dark">
      {/* Header */}
      <header className="flex items-center justify-between px-6 py-4 border-b border-powerbi-border bg-powerbi-card/50 backdrop-blur">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl bg-powerbi-yellow flex items-center justify-center">
            <BarChart3 size={20} className="text-black" />
          </div>
          <div>
            <h1 className="text-base font-semibold text-white leading-tight">Power BI Copilot</h1>
            {activeTab === 'powerbi' && selectedDatasetName && (
              <p className="text-xs text-slate-400">{selectedDatasetName}</p>
            )}
            {activeTab === 'data-engineering' && (
              <p className="text-xs text-slate-400">Databricks</p>
            )}
            {activeTab === 'data-maturity' && (
              <p className="text-xs text-slate-400">DAMA-DMBOK</p>
            )}
          </div>
        </div>
        <div className="flex items-center gap-3">
          {/* Navigation tabs */}
          <nav className="flex items-center bg-powerbi-card border border-powerbi-border rounded-lg p-0.5 gap-0.5">
            <button
              onClick={() => setActiveTab('powerbi')}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
                activeTab === 'powerbi'
                  ? 'bg-powerbi-yellow text-black'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              <BarChart3 size={13} />
              Power BI
            </button>
            <button
              onClick={() => setActiveTab('data-engineering')}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
                activeTab === 'data-engineering'
                  ? 'bg-powerbi-yellow text-black'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              <Database size={13} />
              Engenharia de Dados
            </button>
            <button
              onClick={() => setActiveTab('data-maturity')}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
                activeTab === 'data-maturity'
                  ? 'bg-powerbi-yellow text-black'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              <ShieldCheck size={13} />
              Maturidade de Dados
            </button>
          </nav>
          {activeTab === 'powerbi' && <SyncButton onSync={loadWorkspaces} />}
        </div>
      </header>

      {activeTab === 'powerbi' && (
        <>
          {/* Selectors */}
          <div className="px-6 py-3 border-b border-powerbi-border bg-powerbi-card/30 grid grid-cols-2 gap-3">
            <WorkspaceSelector
              workspaces={workspaces}
              selected={selectedWorkspace}
              onChange={handleWorkspaceChange}
              loading={loadingWorkspaces}
            />
            <DatasetSelector
              datasets={datasets}
              selected={selectedDataset}
              onChange={setSelectedDataset}
              loading={loadingDatasets}
            />
          </div>

          {/* Messages */}
          <div className="flex-1 overflow-y-auto px-6 py-4 space-y-4 scrollbar-thin">
            {messages.map((msg) => (
              <MessageBubble key={msg.id} message={msg} />
            ))}
            {loading && (
              <div className="flex gap-3">
                <div className="w-8 h-8 rounded-full bg-blue-500/20 border border-blue-500/30 flex items-center justify-center">
                  <Loader2 size={16} className="text-blue-400 animate-spin" />
                </div>
                <div className="bg-powerbi-card border border-powerbi-border rounded-2xl rounded-tl-sm px-4 py-3">
                  <div className="flex gap-1">
                    <span className="w-2 h-2 rounded-full bg-slate-400 animate-bounce" style={{ animationDelay: '0ms' }} />
                    <span className="w-2 h-2 rounded-full bg-slate-400 animate-bounce" style={{ animationDelay: '150ms' }} />
                    <span className="w-2 h-2 rounded-full bg-slate-400 animate-bounce" style={{ animationDelay: '300ms' }} />
                  </div>
                </div>
              </div>
            )}
            <div ref={bottomRef} />
          </div>

          {/* Input */}
          <ChatInput
            onSend={handleSend}
            loading={loading}
            disabled={!selectedDataset}
          />
        </>
      )}

      {activeTab === 'data-engineering' && (
        <div className="flex-1 overflow-hidden flex flex-col">
          <DataEngineering />
        </div>
      )}

      {activeTab === 'data-maturity' && (
        <div className="flex-1 overflow-hidden flex flex-col">
          <DataMaturity />
        </div>
      )}
    </div>
  );
}
