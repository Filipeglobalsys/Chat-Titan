export interface Workspace {
  id: string;
  name: string;
  type: string;
}

export interface Dataset {
  id: string;
  workspace_id: string;
  name: string;
  configured_by: string;
}

export interface ChatMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  daxQuery?: string;
  rows?: Record<string, unknown>[];
  rowCount?: number;
  timestamp: Date;
}

export type AppTab = 'powerbi' | 'data-engineering' | 'data-maturity';

export interface DatabricksProgressStep {
  key: string;
  message: string;
  status: 'running' | 'done' | 'error';
}

export interface DatabricksSseEvent {
  type: 'progress' | 'analysis_start' | 'analysis_chunk' | 'done' | 'error';
  step?: string;
  message?: string;
  text?: string;
}
