# CLAUDE.md - Previsão de Vendas API

## O QUE É ESTE PROJETO

API de projeção de vendas para a empresa PRATICMIX (Grupo Manirê, distribuidor de FLV - frutas, legumes e verduras). Prevê quanto cada cliente vai comprar de cada produto nos próximos dias, baseado no padrão de compra por dia da semana. Alimenta o módulo "Previsão de Vendas" (submódulo do PCP) no ERP construído no Lovable.

**Stack:** Python 3.12 + FastAPI, hospedado no Railway, auto-deploy via push no GitHub (branch main).
**Banco:** Supabase (PostgreSQL) - projeto ID: `oeegjfyzwflgqeqpjylc`, região sa-east-1
**Frontend:** Lovable (React/TSX) consome essa API via REST
**Repo:** `AndreMaluf-Manire/previsao-vendas-api`

## LÓGICA CORE (v1.5)

1. Recebe: `dias_frente` (1-7), `semanas_historico` (2-12), `clientes_excluidos`, `data_inicio`, `presenca_minima` (0.0-1.0)
2. Para cada dia alvo (ex: próxima quarta), busca vendas das últimas N quartas-feiras no Supabase
3. Agrupa por cliente → produto → data, somando quantidades do mesmo dia (ignora `descricao_item` de propósito — descrições variam e duplicavam linhas)
4. **Média ponderada com zeros**: semanas sem compra entram como 0. Pesos [N, N-1, ..., 1], mais recente pesa mais. Ex: comprou [10, 0, 8, 0] → (10×4 + 0×3 + 8×2 + 0×1)/10 = 5.6
5. Filtro de presença opcional (`presenca_minima`, default 0 = desligado)
6. Retorna projeção detalhada (por cliente), consolidada (por produto) e CSV

### Tabela fonte: `vendas_itens_importados`
- Colunas usadas: `data_venda` (date), `cliente` (text), `produto` (text), `quantidade` (numeric), `empresa` (text)
- Filtro fixo: `empresa = 'PRATICMIX'`
- ~42k registros, dados desde jan/2026, importados do ERP Conta Azul via N8N
- Outras colunas existentes (não usadas): descricao_item, valor_unitario, valor_total_item, numero_venda, venda_id, item_id, produto_id

### Endpoints:
- `POST /projecao` — projeção detalhada por cliente/item 🔒
- `POST /projecao/consolidado` — projeção consolidada por item (soma clientes) 🔒
- `POST /projecao/download` — download CSV (decimal com vírgula pro Excel BR) 🔒
- `GET /clientes` — lista de clientes únicos (últimos 60 dias) → `{clientes: [], total: N}` 🔒
- `GET /health` — health check (sempre aberto; campo `auth` mostra se a API key está ativa)

🔒 = exige header `X-API-Key` quando a env `API_KEY` está setada (v1.5.1). `/debug/contagem` foi removido na v1.5.1.

### Segurança da API key (nota honesta)
A chave é enviada pelo frontend Lovable (SPA) — ela fica **visível no bundle do navegador** para quem inspecionar. Isso barra acesso casual e scanners, NÃO um atacante dedicado. Solução definitiva documentada como refinamento futuro: proxy via Supabase Edge Function guardando a chave como secret do lado do servidor.

## HISTÓRICO DE VERSÕES E DECISÕES (importante entender antes de mexer)

- **v1.0-1.1**: média simples das semanas COM compra + paginação Supabase (client limita 1000 rows/query — sempre paginar com .range())
- **v1.2**: removido `descricao_item` do agrupamento (variações de texto duplicavam projeções do mesmo produto)
- **v1.3**: filtro de presença 50% (cliente precisava comprar em ≥50% das semanas). Motivo: média parcial inflava — cliente que comprou 60un em 1 de 4 quartas era projetado com 60
- **v1.4**: média ponderada COM ZEROS (a correção certa do problema da v1.3). Comprou 60 em 1 de 4 semanas → projeta 24, não 60
- **v1.5**: filtro de presença REMOVIDO por default (agora configurável via `presenca_minima` no request). Backtest de 12 dias (jun/jul 2026) provou: com a média-com-zeros, o filtro punia duas vezes e causava subestimação de até -34% em dias de crescimento. Erro médio absoluto: 18.6% com filtro → 15.6% sem. Também corrigido decimal do CSV (ponto → vírgula).
- **v1.5.1**: (1) autenticação opt-in por API key — header `X-API-Key` exigido em todos os endpoints exceto `/health`, mas SÓ quando a env `API_KEY` está setada no Railway (sem a env = modo aberto, o que permitiu transição sem downtime); comparação com `secrets.compare_digest`. (2) Ordenação determinística na paginação (`.order(data_venda, venda_id, item_id, id)` antes do `.range()`) — sem order, o PostgREST não garante ordem entre páginas e escrita concorrente (import n8n) podia duplicar/pular linhas. (3) Removido `/debug/contagem` (temporário, expunha dados sem necessidade). Fórmula intocada.

### O que JÁ FOI TESTADO E REPROVADO (não reimplementar sem novo backtest):
- **Fator de tendência** (últimos 14 dias / 14 anteriores como multiplicador): PIOROU o erro (18%). Tendência atrasada amplifica na direção errada.
- **Filtro fixo de presença 50%**: descartado pela razão acima.

## COMO FAZER BACKTEST (método validado)

Pegar datas passadas, aplicar a fórmula como se estivesse projetando (histórico = D-7, D-14, D-21, D-28), comparar com o real do dia D. Métricas: erro % por dia e erro médio absoluto. O Supabase tem os dados reais — qualquer mudança na fórmula deve ser validada assim ANTES de subir.

## CONTEXTO DO NEGÓCIO

- Restaurantes compram FLV com padrão semanal (cardápios mudam por dia da semana)
- A projeção alimenta decisões de COMPRA do PCP — melhor sobrar um pouco que faltar (por isso ceil no arredondado)
- Quantidades em KG ou unidades dependendo do produto (não há coluna de unidade)
- Ruído natural: dias atípicos com ±40% do padrão existem e nenhuma fórmula pega. Piso realista de erro médio: ~10-15%
- Erro médio atual: ~15.6% no agregado diário, viés próximo de zero

## PONTOS DE REFINAMENTO FUTUROS (ideias, nenhuma validada ainda)

1. Detecção de outliers por item (pedido atípico distorce a média — ex: evento pontual)
2. Tratamento de feriados (excluir semanas de carnaval/natal do histórico)
3. Expandir para outras empresas (MANIRÊ, FRULEVE) — hoje EMPRESA é hardcoded
4. Modelo por cliente (clientes grandes têm padrão mais estável que pequenos)
5. Proxy de autenticação via Supabase Edge Function (chave como secret server-side, front chama a function autenticado pelo Supabase Auth) — resolve a exposição da API key no bundle do SPA

### Features removidas na v1.5.0 — candidatas a backtest futuro

O commit anterior ("v1.5 - projecao inteligente", 13a0637, ficou ~2 meses em produção) tinha três filtros de fórmula que foram REMOVIDOS na v1.5.0 sem backtest isolado — o backtest de 12 dias comparou a fórmula limpa contra a produção com TODOS os filtros ligados, então não se sabe o efeito de cada um sozinho. Documentados aqui para não perder a lógica; qualquer retorno exige backtest individual (método acima). O código completo está no commit 13a0637.

1. **Cap de outlier** (`cap_outlier_multiplo`, default era 2.5) — a mais promissora. Por par cliente×produto: se havia ≥3 compras não-zero no histórico, calculava a mediana delas; qualquer compra > mediana × 2.5 era substituída por mediana × 2 ANTES da média ponderada. Com <3 compras não fazia nada (mediana não confiável). `0` desligava. Item respondia `outlier_capado: bool` e o response somava `outliers_capados`.

2. **Filtro de recencidade** (`excluir_inativo_semanas`, default era 2) — excluía o CLIENTE inteiro se ele não comprou em nenhuma das últimas N semanas-alvo (as N datas mais recentes do histórico daquele dia da semana). `0` desligava; validação exigia `0 ≤ N ≤ semanas_historico`. Response somava `clientes_inativos_excluidos`. Risco conhecido: some com cliente que pulou 2 semanas (férias) mas vai voltar.

3. **Filtro de presença por PRODUTO** (`presenca_minima_produto`, default era 0.5) — igual ao filtro de presença do cliente, mas por par cliente×produto: se o produto apareceu em menos de X% das semanas do histórico daquele cliente, a linha era cortada ("comprou 1 vez"). Response somava `produtos_esporadicos_excluidos`; item respondia `semanas_com_produto` e `pct_presenca_produto`. Suspeita: sofre do mesmo problema do filtro de cliente (a média-com-zeros já dilui compra esporádica — filtrar por cima pune duas vezes e subestima).

O que a v1.5.0 MANTEVE dessa versão (higiene, não fórmula): normalização de produto (`strip().upper()`, descarta vazio), descarte de `quantidade ≤ 0` e validação de request nos 3 endpoints POST.

## VARIÁVEIS DE AMBIENTE (Railway)

- `SUPABASE_URL`: https://oeegjfyzwflgqeqpjylc.supabase.co
- `SUPABASE_SERVICE_KEY`: service_role key do Supabase (NUNCA commitar)
- `API_KEY`: (opcional, v1.5.1) liga a exigência do header `X-API-Key`. NUNCA commitar. Ordem crítica ao rotacionar/ativar: primeiro o front (Lovable) enviando a chave nova, DEPOIS a env no Railway — o contrário quebra a tela.
- `PORT`: injetado pelo Railway (Dockerfile usa sh -c pra expandir ${PORT:-8080})

## CONSUMIDORES DA API (todos precisam da X-API-Key quando a auth estiver ativa)

1. **Frontend Lovable** — `src/components/erp/modules/PcpPrevisaoVendas.tsx` (projeto 0dffdc8e-08d0-4270-8044-afdd32e16ef4): 4 fetches diretos (`/clientes`, `/projecao`, `/projecao/consolidado`, `/projecao/download`). Header adicionado em 06/07/2026 (const `API_KEY` no componente — ver nota de segurança acima).
2. **Edge Function `gerar-previsao-vendas`** (Supabase oeegjfyzwflgqeqpjylc, `verify_jwt=false`) — é quem o botão "Gerar Projeção" e o **cron diário 06:00 BRT** realmente usam; chama `/clientes`, `/projecao` e `/projecao/consolidado` e sobrepõe o cache `previsao_vendas_cache`. Atualizada (v5) para enviar o header lido do secret **`PREVISAO_API_KEY`** — enquanto o secret não existir, não envia nada (modo aberto segue funcionando).

**Ordem de ativação da auth (crítica):** 1º front Lovable publicado enviando o header → 2º secret `PREVISAO_API_KEY` nas Edge Functions do Supabase → 3º só então a env `API_KEY` no Railway. Inverter = tela quebrada e/ou cron das 06:00 falhando em silêncio.

## COMO TESTAR EM PRODUÇÃO

- Swagger: https://previsao-vendas-api-production.up.railway.app/docs
- Health: https://previsao-vendas-api-production.up.railway.app/health
- Debug: https://previsao-vendas-api-production.up.railway.app/debug/contagem

## REGRAS DE OURO

1. Push na main = deploy automático no Railway. Não subir código sem validar sintaxe.
2. Mudou a fórmula? Backtest primeiro, deploy depois.
3. Mudou o contrato da API (campos de request/response)? O Lovable precisa ser atualizado junto — avisar o André.
4. Supabase client: SEMPRE paginar com .range() — o limite silencioso de 1000 rows já causou bug em produção (projeção calculada com 1 semana de dados achando que era tudo).
