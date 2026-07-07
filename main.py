from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime, timedelta
from supabase import create_client
import os
import io
import csv
import math
import secrets

app = FastAPI(title="Previsão de Vendas API", version="1.5.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── SUPABASE ─────────────────────────────────────────────

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://oeegjfyzwflgqeqpjylc.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

TABELA = "vendas_itens_importados"
EMPRESA = "PRATICMIX"
PAGE_SIZE = 1000

def get_supabase():
    if not SUPABASE_KEY:
        raise HTTPException(status_code=500, detail="SUPABASE_SERVICE_KEY não configurada")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ─── AUTENTICAÇÃO (v1.5.1) ────────────────────────────────
# Opt-in via env: sem API_KEY setada no Railway = modo aberto (zero downtime
# na transição). Com API_KEY setada, todos os endpoints exceto /health exigem
# o header X-API-Key com o valor exato.

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def exigir_api_key(api_key: Optional[str] = Security(_api_key_header)):
    chave_configurada = os.environ.get("API_KEY")
    if not chave_configurada:
        return  # modo aberto
    if not api_key or not secrets.compare_digest(api_key, chave_configurada):
        raise HTTPException(status_code=401, detail="X-API-Key ausente ou inválida")


# ─── MODELS ───────────────────────────────────────────────

class ProjecaoRequest(BaseModel):
    dias_frente: int = 4
    clientes_excluidos: list[str] = []
    semanas_historico: int = 4
    data_inicio: Optional[str] = None
    # v1.5: filtro de presença agora é opcional e configurável.
    # 0.0 = sem filtro (default, validado por backtest: erro médio 15.6% vs 18.6% com filtro)
    # 0.5 = comportamento antigo da v1.4
    presenca_minima: float = 0.0

class ProjecaoItem(BaseModel):
    data: str
    dia_semana: str
    dia_semana_num: int
    cliente: str
    produto: str
    quantidade_projetada: float
    quantidade_arredondada: int
    semanas_com_dados: int
    is_cliente_novo: bool
    pct_presenca: int

class ProjecaoDia(BaseModel):
    data: str
    dia_semana: str
    dia_semana_num: int
    total_itens_projetados: int
    total_quantidade: float
    itens: list[ProjecaoItem]

class ProjecaoResponse(BaseModel):
    dias: list[ProjecaoDia]
    total_geral_quantidade: float
    total_clientes: int
    total_itens_unicos: int
    total_registros_historico: int
    clientes_filtrados: int
    gerado_em: str


# ─── HELPERS ──────────────────────────────────────────────

DIAS_SEMANA = {
    0: "Segunda-feira",
    1: "Terça-feira",
    2: "Quarta-feira",
    3: "Quinta-feira",
    4: "Sexta-feira",
    5: "Sábado",
    6: "Domingo",
}

def get_dia_semana_nome(d: date) -> str:
    return DIAS_SEMANA[d.weekday()]

def calcular_datas_historico(data_alvo: date, semanas: int) -> list[date]:
    return [data_alvo - timedelta(weeks=i) for i in range(1, semanas + 1)]

def normalizar_produto(p) -> str:
    return (p or "").strip().upper()

def media_ponderada_com_zeros(datas_qtd: dict, datas_historico_ordenadas: list[str]) -> float:
    """
    Média ponderada considerando TODAS as semanas históricas.
    Semanas sem compra entram como 0. Mais recente = mais peso.
    Ex: 4 semanas, comprou em 2 → valores [10, 0, 8, 0], pesos [4, 3, 2, 1]
    Resultado: (10×4 + 0×3 + 8×2 + 0×1) / 10 = 5.6
    """
    n = len(datas_historico_ordenadas)
    if n == 0:
        return 0.0

    pesos = list(range(n, 0, -1))
    soma_ponderada = 0.0
    soma_pesos = sum(pesos)

    for i, data_str in enumerate(datas_historico_ordenadas):
        qtd = datas_qtd.get(data_str, 0.0)
        soma_ponderada += qtd * pesos[i]

    return soma_ponderada / soma_pesos


# ─── DATA ACCESS (COM PAGINAÇÃO) ─────────────────────────

def buscar_vendas_periodo(supabase, data_inicio: date, data_fim: date) -> list[dict]:
    todas_vendas = []
    offset = 0

    while True:
        result = supabase.table(TABELA) \
            .select("data_venda, cliente, produto, quantidade") \
            .eq("empresa", EMPRESA) \
            .gte("data_venda", data_inicio.isoformat()) \
            .lte("data_venda", data_fim.isoformat()) \
            .order("data_venda") \
            .order("venda_id") \
            .order("item_id") \
            .order("id") \
            .range(offset, offset + PAGE_SIZE - 1) \
            .execute()

        dados = result.data if result.data else []
        todas_vendas.extend(dados)

        if len(dados) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    return todas_vendas


def buscar_todos_clientes(supabase) -> list[str]:
    data_corte = (date.today() - timedelta(days=60)).isoformat()

    todos = []
    offset = 0

    while True:
        result = supabase.table(TABELA) \
            .select("cliente") \
            .eq("empresa", EMPRESA) \
            .gte("data_venda", data_corte) \
            .order("data_venda") \
            .order("venda_id") \
            .order("item_id") \
            .order("id") \
            .range(offset, offset + PAGE_SIZE - 1) \
            .execute()

        dados = result.data if result.data else []
        todos.extend(dados)

        if len(dados) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    return sorted(set(row["cliente"] for row in todos))


# ─── CORE LOGIC ───────────────────────────────────────────

def calcular_projecao(
    vendas: list[dict],
    data_alvo: date,
    semanas_historico: int,
    clientes_excluidos: list[str],
    presenca_minima: float = 0.0,
) -> tuple[ProjecaoDia, int]:

    datas_historico = calcular_datas_historico(data_alvo, semanas_historico)
    datas_historico_str = set(d.isoformat() for d in datas_historico)
    datas_ordenadas = sorted(datas_historico_str, reverse=True)
    excluidos = set(clientes_excluidos)

    agrupado = {}
    datas_por_cliente = {}

    for venda in vendas:
        data_str = str(venda["data_venda"])[:10]
        if data_str not in datas_historico_str:
            continue

        cliente = venda["cliente"]
        if cliente in excluidos:
            continue

        # Normaliza produto ("Tomate " == "TOMATE") e descarta vazio
        produto = normalizar_produto(venda["produto"])
        if not produto:
            continue

        # Descarta qtd zero/negativa (devolução não entra na média)
        qtd = float(venda["quantidade"] or 0)
        if qtd <= 0:
            continue

        if cliente not in agrupado:
            agrupado[cliente] = {}
            datas_por_cliente[cliente] = set()

        datas_por_cliente[cliente].add(data_str)

        if produto not in agrupado[cliente]:
            agrupado[cliente][produto] = {}

        if data_str not in agrupado[cliente][produto]:
            agrupado[cliente][produto][data_str] = 0.0
        agrupado[cliente][produto][data_str] += qtd

    itens_projecao = []
    dia_semana_nome = get_dia_semana_nome(data_alvo)
    dia_semana_num = data_alvo.weekday()
    clientes_filtrados = 0

    for cliente, produtos in agrupado.items():
        semanas_com_dados = len(datas_por_cliente[cliente])
        pct_presenca = semanas_com_dados / semanas_historico

        # v1.5: filtro opcional (default 0 = sem filtro)
        if presenca_minima > 0 and pct_presenca < presenca_minima:
            clientes_filtrados += 1
            continue

        is_novo = pct_presenca < 1.0
        pct_int = round(pct_presenca * 100)

        for produto, datas_qtd in produtos.items():
            qtd_projetada = media_ponderada_com_zeros(datas_qtd, datas_ordenadas)

            itens_projecao.append(ProjecaoItem(
                data=data_alvo.isoformat(),
                dia_semana=dia_semana_nome,
                dia_semana_num=dia_semana_num,
                cliente=cliente,
                produto=produto,
                quantidade_projetada=round(qtd_projetada, 3),
                quantidade_arredondada=math.ceil(qtd_projetada),
                semanas_com_dados=semanas_com_dados,
                is_cliente_novo=is_novo,
                pct_presenca=pct_int,
            ))

    itens_projecao.sort(key=lambda x: (x.cliente, x.produto))
    total_quantidade = sum(i.quantidade_projetada for i in itens_projecao)

    return ProjecaoDia(
        data=data_alvo.isoformat(),
        dia_semana=dia_semana_nome,
        dia_semana_num=dia_semana_num,
        total_itens_projetados=len(itens_projecao),
        total_quantidade=round(total_quantidade, 3),
        itens=itens_projecao,
    ), clientes_filtrados


# ─── VALIDAÇÃO ────────────────────────────────────────────

def _validar_request(req: ProjecaoRequest) -> date:
    """Valida o request e devolve a data_base já parseada."""
    if not 1 <= req.dias_frente <= 7:
        raise HTTPException(400, "dias_frente deve ser entre 1 e 7")
    if not 2 <= req.semanas_historico <= 12:
        raise HTTPException(400, "semanas_historico deve ser entre 2 e 12")
    if not 0.0 <= req.presenca_minima <= 1.0:
        raise HTTPException(400, "presenca_minima deve ser entre 0.0 e 1.0")
    if req.data_inicio:
        try:
            return date.fromisoformat(req.data_inicio)
        except ValueError:
            raise HTTPException(400, "data_inicio inválida — use o formato YYYY-MM-DD")
    return date.today() + timedelta(days=1)


# ─── ENDPOINTS ────────────────────────────────────────────

@app.post("/projecao", response_model=ProjecaoResponse, dependencies=[Depends(exigir_api_key)])
async def gerar_projecao(req: ProjecaoRequest):
    data_base = _validar_request(req)

    supabase = get_supabase()

    data_mais_antiga = data_base - timedelta(weeks=req.semanas_historico, days=1)

    vendas = buscar_vendas_periodo(supabase, data_mais_antiga, date.today())

    dias = []
    todos_clientes = set()
    todos_itens = set()
    total_filtrados = 0

    for i in range(req.dias_frente):
        data_alvo = data_base + timedelta(days=i)
        projecao_dia, filtrados = calcular_projecao(vendas, data_alvo, req.semanas_historico, req.clientes_excluidos, req.presenca_minima)
        dias.append(projecao_dia)
        total_filtrados += filtrados

        for item in projecao_dia.itens:
            todos_clientes.add(item.cliente)
            todos_itens.add(item.produto)

    return ProjecaoResponse(
        dias=dias,
        total_geral_quantidade=round(sum(d.total_quantidade for d in dias), 3),
        total_clientes=len(todos_clientes),
        total_itens_unicos=len(todos_itens),
        total_registros_historico=len(vendas),
        clientes_filtrados=total_filtrados,
        gerado_em=datetime.now().isoformat(),
    )


@app.post("/projecao/consolidado", dependencies=[Depends(exigir_api_key)])
async def projecao_consolidada(req: ProjecaoRequest):
    data_base = _validar_request(req)

    supabase = get_supabase()

    data_mais_antiga = data_base - timedelta(weeks=req.semanas_historico, days=1)

    vendas = buscar_vendas_periodo(supabase, data_mais_antiga, date.today())

    consolidado = []

    for i in range(req.dias_frente):
        data_alvo = data_base + timedelta(days=i)
        projecao_dia, _ = calcular_projecao(vendas, data_alvo, req.semanas_historico, req.clientes_excluidos, req.presenca_minima)

        itens_consolidados = {}
        for p in projecao_dia.itens:
            if p.produto not in itens_consolidados:
                itens_consolidados[p.produto] = 0.0
            itens_consolidados[p.produto] += p.quantidade_projetada

        for produto, qtd in sorted(itens_consolidados.items()):
            consolidado.append({
                "data": data_alvo.isoformat(),
                "dia_semana": get_dia_semana_nome(data_alvo),
                "produto": produto,
                "quantidade_projetada": round(qtd, 3),
                "quantidade_arredondada": math.ceil(qtd),
            })

    return consolidado


@app.post("/projecao/download", dependencies=[Depends(exigir_api_key)])
async def download_projecao(req: ProjecaoRequest):
    data_base = _validar_request(req)

    supabase = get_supabase()

    data_mais_antiga = data_base - timedelta(weeks=req.semanas_historico, days=1)

    vendas = buscar_vendas_periodo(supabase, data_mais_antiga, date.today())

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["DATA", "DIA_SEMANA", "CLIENTE", "PRODUTO", "QTD_PROJETADA", "QTD_ARREDONDADA", "SEMANAS_HISTORICO", "PCT_PRESENCA", "CLIENTE_NOVO"])

    for i in range(req.dias_frente):
        data_alvo = data_base + timedelta(days=i)
        projecao_dia, _ = calcular_projecao(vendas, data_alvo, req.semanas_historico, req.clientes_excluidos, req.presenca_minima)

        for item in projecao_dia.itens:
            writer.writerow([
                item.data,
                item.dia_semana,
                item.cliente,
                item.produto,
                str(item.quantidade_projetada).replace(".", ","),  # decimal BR pro Excel
                item.quantidade_arredondada,
                item.semanas_com_dados,
                f"{item.pct_presenca}%",
                "Sim" if item.is_cliente_novo else "Não",
            ])

    output.seek(0)

    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8-sig")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=previsao_vendas_{date.today().isoformat()}.csv"}
    )


@app.get("/clientes", dependencies=[Depends(exigir_api_key)])
async def listar_clientes():
    supabase = get_supabase()
    clientes = buscar_todos_clientes(supabase)
    return {"clientes": clientes, "total": len(clientes)}


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": "1.5.1",
        "empresa": EMPRESA,
        "presenca_minima_default": "0 (sem filtro)",
        "auth": "ativa" if os.environ.get("API_KEY") else "aberta (API_KEY não setada)",
    }
