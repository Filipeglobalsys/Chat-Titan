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
