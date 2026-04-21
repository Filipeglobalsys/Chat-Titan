# Titan BI — Copilot Power BI

Assistente de dados com IA para Power BI: faça perguntas em linguagem natural e receba respostas executivas geradas automaticamente via DAX.

## Stack

- **Backend**: Python 3.11 + FastAPI
- **Frontend**: HTML/CSS/JS single-page (servido pelo próprio FastAPI)
- **Banco de metadados**: Supabase (PostgreSQL)
- **IA**: Anthropic Claude (claude-sonnet-4-6 por padrão)
- **Auth**: Supabase Auth (email/senha) + Microsoft Entra ID (service principal para Power BI API)

## Pré-requisitos

- Python 3.11+
- Conta no [Supabase](https://supabase.com)
- App Registration no Azure (Entra ID) com permissão `Dataset.ReadWrite.All`
- Chave da [Anthropic API](https://console.anthropic.com)

## Configuração local

```bash
cd app
cp .env.example .env
# Preencha .env com suas credenciais
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Acesse: http://localhost:8000

## Variáveis de Ambiente

| Variável | Descrição |
|----------|-----------|
| `TENANT_ID` | ID do tenant Azure |
| `CLIENT_ID` | ID do app registration |
| `CLIENT_SECRET` | Secret do app registration |
| `SUPABASE_URL` | URL do projeto Supabase |
| `SUPABASE_SERVICE_ROLE_KEY` | Chave service role do Supabase |
| `ANTHROPIC_API_KEY` | Chave da Anthropic |
| `ANTHROPIC_MODEL` | Modelo Claude (padrão: `claude-sonnet-4-6`) |
| `PORT` | Porta do servidor (padrão: `8000`) |

## Deploy na Vercel

1. Importe o repositório no [Vercel](https://vercel.com)
2. Configure todas as variáveis de ambiente acima em **Project Settings → Environment Variables**
3. O deploy é automático — `vercel.json` já está configurado

> **Atenção**: funcionalidades de gateway SQL (datasets on-premises) não persistem configuração entre invocações serverless. Para uso completo, faça deploy em servidor dedicado (ex.: Railway, Fly.io, VPS).

## Banco de Dados (Supabase)

Execute as migrações em **SQL Editor** no painel do Supabase:

```
supabase/migrations/
```

## Arquitetura

```
Pergunta (PT)
     ↓
 Claude AI
 (schema do dataset como contexto)
     ↓
 Query DAX gerada
     ↓
 Power BI executeQueries API
     ↓
 Dados retornados
     ↓
 Claude formata resposta executiva
     ↓
 Usuário vê resposta + tabela
```
