# Power BI Copilot

Consulte datasets do Power BI usando linguagem natural, com geração automática de DAX via OpenAI.

## Stack

- **Backend**: Node.js + Express
- **Frontend**: Next.js 14 + Tailwind CSS
- **Banco**: Supabase (PostgreSQL)
- **IA**: OpenAI GPT-4o
- **Auth**: Microsoft Entra ID (OAuth2 Client Credentials)

## Configuração Inicial

### 1. Pré-requisitos

- Node.js 20+
- Conta no [Supabase](https://supabase.com)
- App Registration no Azure (Entra ID) com permissão `Dataset.ReadWrite.All`
- Chave da OpenAI API

### 2. Azure App Registration

No [Azure Portal](https://portal.azure.com):

1. Acesse **Azure Active Directory → App registrations → New registration**
2. Defina um nome, clique em **Register**
3. Anote o **Application (client) ID** e o **Directory (tenant) ID**
4. Vá em **Certificates & secrets → New client secret** — anote o valor
5. Vá em **API permissions → Add a permission → Power BI Service**
6. Adicione: `Dataset.ReadWrite.All`, `Workspace.Read.All`
7. Clique em **Grant admin consent**

### 3. Supabase

1. Crie um projeto em [supabase.com](https://supabase.com)
2. Acesse **SQL Editor** e execute o conteúdo de `supabase/migrations/001_initial_schema.sql`
3. Anote a **URL do projeto** e a **service_role key** (em Project Settings → API)

### 4. Backend

```bash
cd backend
cp .env.example .env
# Edite .env com suas credenciais
npm install
npm run dev
```

### 5. Frontend

```bash
cd frontend
cp .env.local.example .env.local
# NEXT_PUBLIC_API_URL=http://localhost:3001
npm install
npm run dev
```

Acesse: http://localhost:3000

## Uso

1. **Sincronizar Metadados**: Clique em "Sincronizar Metadados" na barra superior para importar workspaces, datasets, tabelas e medidas do Power BI para o Supabase
2. **Selecionar Workspace e Dataset**: Use os dropdowns no topo
3. **Fazer Perguntas**: Digite perguntas em português ou inglês no chat

### Exemplos de perguntas

- "Qual o total de vendas em 2024?"
- "Top 5 clientes por receita"
- "Compare vendas por região no último trimestre"
- "Mostre a evolução mensal de novos clientes"

## Arquitetura

```
Pergunta (PT/EN)
      ↓
  OpenAI GPT-4o
  (Schema do dataset como contexto)
      ↓
  Query DAX gerada
      ↓
  Power BI executeQueries API
      ↓
  Dados retornados
      ↓
  OpenAI formata resposta
      ↓
  Usuário vê resposta + tabela + DAX
```

## Variáveis de Ambiente

### Backend (`backend/.env`)

| Variável | Descrição |
|----------|-----------|
| `TENANT_ID` | ID do tenant Azure |
| `CLIENT_ID` | ID do app registration |
| `CLIENT_SECRET` | Secret do app registration |
| `SUPABASE_URL` | URL do projeto Supabase |
| `SUPABASE_SERVICE_ROLE_KEY` | Chave service role do Supabase |
| `OPENAI_API_KEY` | Chave da OpenAI |
| `OPENAI_MODEL` | Modelo (padrão: `gpt-4o`) |
| `PORT` | Porta do backend (padrão: `3001`) |
| `CORS_ORIGIN` | Origem permitida (padrão: `http://localhost:3000`) |

### Frontend (`frontend/.env.local`)

| Variável | Descrição |
|----------|-----------|
| `NEXT_PUBLIC_API_URL` | URL do backend |
