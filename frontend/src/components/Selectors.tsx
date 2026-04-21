'use client';
import { ChevronDown, Database, LayoutGrid } from 'lucide-react';
import { Workspace, Dataset } from '@/types';

interface WorkspaceSelectorProps {
  workspaces: Workspace[];
  selected: string;
  onChange: (id: string) => void;
  loading: boolean;
}

export function WorkspaceSelector({ workspaces, selected, onChange, loading }: WorkspaceSelectorProps) {
  return (
    <div className="relative">
      <LayoutGrid size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
      <select
        value={selected}
        onChange={(e) => onChange(e.target.value)}
        disabled={loading}
        className="w-full pl-9 pr-8 py-2 bg-powerbi-card border border-powerbi-border rounded-lg text-sm text-slate-200 appearance-none focus:outline-none focus:border-powerbi-yellow/60 disabled:opacity-50"
      >
        <option value="">Selecione um Workspace</option>
        {workspaces.map((ws) => (
          <option key={ws.id} value={ws.id}>
            {ws.name}
          </option>
        ))}
      </select>
      <ChevronDown size={14} className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none" />
    </div>
  );
}

interface DatasetSelectorProps {
  datasets: Dataset[];
  selected: string;
  onChange: (id: string) => void;
  loading: boolean;
}

export function DatasetSelector({ datasets, selected, onChange, loading }: DatasetSelectorProps) {
  return (
    <div className="relative">
      <Database size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
      <select
        value={selected}
        onChange={(e) => onChange(e.target.value)}
        disabled={loading || !datasets.length}
        className="w-full pl-9 pr-8 py-2 bg-powerbi-card border border-powerbi-border rounded-lg text-sm text-slate-200 appearance-none focus:outline-none focus:border-powerbi-yellow/60 disabled:opacity-50"
      >
        <option value="">Selecione um Dataset</option>
        {datasets.map((ds) => (
          <option key={ds.id} value={ds.id}>
            {ds.name}
          </option>
        ))}
      </select>
      <ChevronDown size={14} className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none" />
    </div>
  );
}
