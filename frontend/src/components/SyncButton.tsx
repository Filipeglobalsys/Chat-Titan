'use client';
import { RefreshCw } from 'lucide-react';
import { useState } from 'react';
import { api } from '@/lib/api';

interface Props {
  onSync: () => void;
}

export function SyncButton({ onSync }: Props) {
  const [syncing, setSyncing] = useState(false);
  const [message, setMessage] = useState('');

  async function handleSync() {
    setSyncing(true);
    setMessage('');
    try {
      const result = await api.syncMetadata();
      setMessage(`Sincronizado: ${result.workspaces} workspace(s)`);
      onSync();
    } catch (err) {
      setMessage('Erro ao sincronizar');
    } finally {
      setSyncing(false);
    }
  }

  return (
    <div className="flex items-center gap-3">
      <button
        onClick={handleSync}
        disabled={syncing}
        className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-powerbi-yellow/10 text-powerbi-yellow border border-powerbi-yellow/30 hover:bg-powerbi-yellow/20 transition-colors text-sm font-medium disabled:opacity-50"
      >
        <RefreshCw size={14} className={syncing ? 'animate-spin' : ''} />
        {syncing ? 'Sincronizando...' : 'Sincronizar Metadados'}
      </button>
      {message && <span className="text-xs text-slate-400">{message}</span>}
    </div>
  );
}
