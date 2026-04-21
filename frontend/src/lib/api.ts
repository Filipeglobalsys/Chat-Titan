const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3001';

async function fetchApi<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || 'API error');
  }
  return res.json();
}

export const api = {
  getWorkspaces: () => fetchApi<{ id: string; name: string; type: string }[]>('/api/workspaces'),

  syncMetadata: () =>
    fetchApi<{ success: boolean; workspaces: number }>('/api/workspaces/sync', {
      method: 'POST',
    }),

  getDatasets: (workspaceId: string) =>
    fetchApi<{ id: string; workspace_id: string; name: string; configured_by: string }[]>(
      `/api/datasets/${workspaceId}`
    ),

  chat: (question: string, datasetId: string) =>
    fetchApi<{
      question: string;
      daxQuery: string;
      rows: Record<string, unknown>[];
      answer: string;
      rowCount: number;
    }>('/api/chat', {
      method: 'POST',
      body: JSON.stringify({ question, datasetId }),
    }),
};
