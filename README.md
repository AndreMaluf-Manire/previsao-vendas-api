# Previsão de Vendas API

API de projeção de vendas baseada em histórico por dia da semana.

## Stack
- Python 3.12 + FastAPI
- Supabase (PostgreSQL)
- Deploy: Railway

## Endpoints
- `POST /projecao` — Projeção detalhada por cliente/item
- `POST /projecao/consolidado` — Projeção consolidada por item
- `POST /projecao/download` — Download CSV
- `GET /clientes` — Lista de clientes
- `GET /health` — Health check

## Variáveis de Ambiente
- `SUPABASE_URL`
- `SUPABASE_SERVICE_KEY`
- `PORT` (default: 8080)
