import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Power BI Copilot',
  description: 'Query Power BI datasets using natural language',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="pt-BR">
      <body>{children}</body>
    </html>
  );
}
