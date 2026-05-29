from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime, timedelta
from supabase import create_client
import os
import io
import csv
import math
import statistics

app = FastAPI(title="Previsão de Vendas API", version="1.5.0")

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

# Defaults v1.5 — todos sobrescrevíveis via request body
PRESENCA_MINIMA_CLIENTE_DEFAULT = 0.5     # cliente precisa aparecer em ≥50% das semanas históricas
PRESENCA_MINIMA_PRODUTO_DEFAULT = 0.5     # produto precisa aparecer em ≥50% das semanas históricas
EXCLUIR_INATIVO_SEMANAS_DEFAULT = 2       # se cliente não comprou nas últimas N semanas-alvo → excluído (0 desliga)
CAP_OUTLIER_MULTIPLO_DEFAULT = 2.5        # compra > X × mediana é limitada a mediana×2 (0 desliga)


def get_supabase():
    if not SUPABASE_KEY:
        raise HTTPException(status_code=500, detail="SUPABASE_SERVICE_KEY não configurada")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ─── MODELS ───────────────────────────────────────────────

class ProjecaoRequest(BaseModel):
    dias_frente: int = 4
    clientes_excluidos: list[str] = []
    semanas_historico: int = 4
    data_inicio: Optional[str] = None
    # v1.5 — parâmetros de inteligência (todos opcionais, com defaults)
    presenca_minima_cliente: float = PRESENCA_MINIMA_CLIENTE_DEFAULT
    presenca_minima_produto: float = PRESENCA_MINIMA_PRODUTO_DEFAULT
    excluir_inativo_semanas: int = EXCLUIR_INATIVO_SEMANAS_DEFAULT
    cap_outlier_multiplo: float = CAP_OUTLIER_MULTIPLO_DEFAULT

class ProjecaoItem(BaseModel):
    data: str
    dia_semana: str
    dia_semana_num: int
    cliente: str
    produto: str
    quantidade_projetada: float
    quantidade_arredondada: int
    semanas_com_dados: int          # quantas semanas do histórico o CLIENTE comprou (qualquer coisa)
    semanas_com_produto: int        # quantas semanas o produto específico aparece (v1.5)
    is_cliente_novo: bool
    pct_presenca: int               # % presença do cliente
    pct_presenca_produto: int       # % presença do produto (v1.5)
    outlier_capado: bool            # houve cap de outlier neste par (v1.5)

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
    clientes_inativos_excluidos: int     # v1.5
    produtos_esporadicos_excluidos: int  # v1.5
    outliers_capados: int                # v1.5
    gerado_em: str
    parametros_usados: dict              # v1.5 — eco dos parâmetros aplicados


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

def formatar_decimal_br(v) -> str:
    """Para CSV: troca ponto por vírgula no decimal (Excel BR)."""
    if isinstance(v, float):
        return f"{v:.3f}".replace(".", ",")
    return str(v)

def aplicar_cap_outlier(datas_qtd: dict, multiplo: float) -> tuple[dict, bool]:
    """
    Se alguma compra é > multiplo × mediana das demais (não-zero), limita a mediana×2.
    Só atua quando há ≥3 compras (não-zero), senão a mediana não é confiável.
    Retorna (dict ajustado, houve_cap).
    """
    if multiplo <= 0:
        return datas_qtd, False
    valores_nz = [v for v in datas_qtd.values() if v > 0]
    if len(valores_nz) < 3:
        return datas_qtd, False
    mediana = statistics.median(valores_nz)
    if mediana <= 0:
        return datas_qtd, False
    limite = mediana * multiplo
    teto = mediana * 2.0
    ajustado = {}
    houve_cap = False
    for k, v in datas_qtd.items():
        if v > limite:
            ajustado[k] = teto
            houve_cap = True
        else:
            ajustado[k] = v
    return ajustado, houve_cap

def media_ponderada_com_zeros(datas_qtd: dict, datas_historico_ordenadas: list[str]) -> float:
    """
    Média ponderada considerando TODAS as semanas históricas.
    Semanas sem compra entram como 0. Mais recente = mais peso.

    Ex: 4 semanas, comprou em 2 → valores [10, 0, 8, 0], pesos [4, 3, 2, 1]
    Resultado: (10×4 + 0×3 + 8×2 + 0×1) / (4+3+2+1) = 5.6
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
    presenca_minima_cliente: float,
    presenca_minima_produto: float,
    excluir_inativo_semanas: int,
    cap_outlier_multiplo: float,
) -> tuple[ProjecaoDia, dict]:
    """
    Retorna (ProjecaoDia, stats) onde stats = {
      'clientes_filtrados': int,         # presença de cliente < mínima
      'clientes_inativos_excluidos': int, # cliente parou de comprar nas últimas N semanas
      'produtos_esporadicos_excluidos': int, # produto < presença mínima
      'outliers_capados': int,
    }
    """

    datas_historico = calcular_datas_historico(data_alvo, semanas_historico)
    datas_historico_str = set(d.isoformat() for d in datas_historico)
    # Ordenadas da mais recente pra mais antiga (pra pesos e recencidade)
    datas_ordenadas = sorted(datas_historico_str, reverse=True)
    datas_recentes_alvo = set(datas_ordenadas[:max(0, excluir_inativo_semanas)])

    agrupado = {}                 # cliente → produto → {data: qtd}
    datas_por_cliente = {}        # cliente → set(data)

    for venda in vendas:
        data_str = str(venda["data_venda"])[:10]
        if data_str not in datas_historico_str:
            continue

        cliente = venda["cliente"]
        if cliente in clientes_excluidos:
            continue

        produto = normalizar_produto(venda["produto"])
        if not produto:
            continue

        qtd = float(venda["quantidade"] or 0)
        if qtd <= 0:
            continue

        if cliente not in agrupado:
            agrupado[cliente] = {}
            datas_por_cliente[cliente] = set()

        datas_por_cliente[cliente].add(data_str)

        if produto not in agrupado[cliente]:
            agrupado[cliente][produto] = {}

        agrupado[cliente][produto][data_str] = agrupado[cliente][produto].get(data_str, 0.0) + qtd

    itens_projecao = []
    dia_semana_nome = get_dia_semana_nome(data_alvo)
    dia_semana_num = data_alvo.weekday()

    stats = {
        "clientes_filtrados": 0,
        "clientes_inativos_excluidos": 0,
        "produtos_esporadicos_excluidos": 0,
        "outliers_capados": 0,
    }

    for cliente, produtos in agrupado.items():
        semanas_com_dados = len(datas_por_cliente[cliente])
        pct_presenca = semanas_com_dados / semanas_historico

        # Filtro 1 — presença mínima do cliente
        if pct_presenca < presenca_minima_cliente:
            stats["clientes_filtrados"] += 1
            continue

        # Filtro 2 — recencidade: cliente comprou em alguma das últimas N semanas-alvo?
        if excluir_inativo_semanas > 0 and datas_recentes_alvo:
            comprou_recente = bool(datas_por_cliente[cliente] & datas_recentes_alvo)
            if not comprou_recente:
                stats["clientes_inativos_excluidos"] += 1
                continue

        is_novo = pct_presenca < 1.0
        pct_int = round(pct_presenca * 100)

        for produto, datas_qtd in produtos.items():
            semanas_com_produto = len(datas_qtd)
            pct_presenca_produto = semanas_com_produto / semanas_historico

            # Filtro 3 — presença mínima do produto (mata "comprou 1 vez")
            if pct_presenca_produto < presenca_minima_produto:
                stats["produtos_esporadicos_excluidos"] += 1
                continue

            # Filtro 4 — cap de outlier
            datas_qtd_ajustado, houve_cap = aplicar_cap_outlier(datas_qtd, cap_outlier_multiplo)
            if houve_cap:
                stats["outliers_capados"] += 1

            qtd_projetada = media_ponderada_com_zeros(datas_qtd_ajustado, datas_ordenadas)

            itens_projecao.append(ProjecaoItem(
                data=data_alvo.isoformat(),
                dia_semana=dia_semana_nome,
                dia_semana_num=dia_semana_num,
                cliente=cliente,
                produto=produto,
                quantidade_projetada=round(qtd_projetada, 3),
                quantidade_arredondada=math.ceil(qtd_projetada),
                semanas_com_dados=semanas_com_dados,
                semanas_com_produto=semanas_com_produto,
                is_cliente_novo=is_novo,
                pct_presenca=pct_int,
                pct_presenca_produto=round(pct_presenca_produto * 100),
                outlier_capado=houve_cap,
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
    ), stats


def _validar_request(req: ProjecaoRequest):
    if not 1 <= req.dias_frente <= 7:
        raise HTTPException(400, "dias_frente deve ser entre 1 e 7")
    if not 2 <= req.semanas_historico <= 12:
        raise HTTPException(400, "semanas_historico deve ser entre 2 e 12")
    if not 0.0 <= req.presenca_minima_cliente <= 1.0:
        raise HTTPException(400, "presenca_minima_cliente deve ser entre 0.0 e 1.0")
    if not 0.0 <= req.presenca_minima_produto <= 1.0:
        raise HTTPException(400, "presenca_minima_produto deve ser entre 0.0 e 1.0")
    if not 0 <= req.excluir_inativo_semanas <= req.semanas_historico:
        raise HTTPException(400, "excluir_inativo_semanas deve ser entre 0 e semanas_historico")
    if req.cap_outlier_multiplo < 0:
        raise HTTPException(400, "cap_outlier_multiplo deve ser >= 0")


def _parametros_eco(req: ProjecaoRequest) -> dict:
    return {
        "presenca_minima_cliente": req.presenca_minima_cliente,
        "presenca_minima_produto": req.presenca_minima_produto,
        "excluir_inativo_semanas": req.excluir_inativo_semanas,
        "cap_outlier_multiplo": req.cap_outlier_multiplo,
        "semanas_historico": req.semanas_historico,
        "dias_frente": req.dias_frente,
    }


# ─── ENDPOINTS ────────────────────────────────────────────

@app.post("/projecao", response_model=ProjecaoResponse)
async def gerar_projecao(req: ProjecaoRequest):
    _validar_request(req)

    supabase = get_supabase()

    data_base = date.fromisoformat(req.data_inicio) if req.data_inicio else date.today() + timedelta(days=1)
    data_mais_antiga = data_base - timedelta(weeks=req.semanas_historico, days=1)

    vendas = buscar_vendas_periodo(supabase, data_mais_antiga, date.today())

    dias = []
    todos_clientes = set()
    todos_itens = set()
    total_filtrados = 0
    total_inativos = 0
    total_esporadicos = 0
    total_outliers = 0

    for i in range(req.dias_frente):
        data_alvo = data_base + timedelta(days=i)
        projecao_dia, stats = calcular_projecao(
            vendas, data_alvo, req.semanas_historico, req.clientes_excluidos,
            req.presenca_minima_cliente, req.presenca_minima_produto,
            req.excluir_inativo_semanas, req.cap_outlier_multiplo,
        )
        dias.append(projecao_dia)
        total_filtrados += stats["clientes_filtrados"]
        total_inativos += stats["clientes_inativos_excluidos"]
        total_esporadicos += stats["produtos_esporadicos_excluidos"]
        total_outliers += stats["outliers_capados"]

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
        clientes_inativos_excluidos=total_inativos,
        produtos_esporadicos_excluidos=total_esporadicos,
        outliers_capados=total_outliers,
        gerado_em=datetime.now().isoformat(),
        parametros_usados=_parametros_eco(req),
    )


@app.post("/projecao/consolidado")
async def projecao_consolidada(req: ProjecaoRequest):
    _validar_request(req)

    supabase = get_supabase()

    data_base = date.fromisoformat(req.data_inicio) if req.data_inicio else date.today() + timedelta(days=1)
    data_mais_antiga = data_base - timedelta(weeks=req.semanas_historico, days=1)

    vendas = buscar_vendas_periodo(supabase, data_mais_antiga, date.today())

    consolidado = []

    for i in range(req.dias_frente):
        data_alvo = data_base + timedelta(days=i)
        projecao_dia, _ = calcular_projecao(
            vendas, data_alvo, req.semanas_historico, req.clientes_excluidos,
            req.presenca_minima_cliente, req.presenca_minima_produto,
            req.excluir_inativo_semanas, req.cap_outlier_multiplo,
        )

        itens_consolidados = {}
        for p in projecao_dia.itens:
            itens_consolidados[p.produto] = itens_consolidados.get(p.produto, 0.0) + p.quantidade_projetada

        for produto, qtd in sorted(itens_consolidados.items()):
            consolidado.append({
                "data": data_alvo.isoformat(),
                "dia_semana": get_dia_semana_nome(data_alvo),
                "produto": produto,
                "quantidade_projetada": round(qtd, 3),
                "quantidade_arredondada": math.ceil(qtd),
            })

    return consolidado


@app.post("/projecao/download")
async def download_projecao(req: ProjecaoRequest):
    _validar_request(req)

    supabase = get_supabase()

    data_base = date.fromisoformat(req.data_inicio) if req.data_inicio else date.today() + timedelta(days=1)
    data_mais_antiga = data_base - timedelta(weeks=req.semanas_historico, days=1)

    vendas = buscar_vendas_periodo(supabase, data_mais_antiga, date.today())

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow([
        "DATA", "DIA_SEMANA", "CLIENTE", "PRODUTO",
        "QTD_PROJETADA", "QTD_ARREDONDADA",
        "SEMANAS_CLIENTE", "PCT_PRESENCA_CLIENTE",
        "SEMANAS_PRODUTO", "PCT_PRESENCA_PRODUTO",
        "CLIENTE_NOVO", "OUTLIER_CAPADO",
    ])

    for i in range(req.dias_frente):
        data_alvo = data_base + timedelta(days=i)
        projecao_dia, _ = calcular_projecao(
            vendas, data_alvo, req.semanas_historico, req.clientes_excluidos,
            req.presenca_minima_cliente, req.presenca_minima_produto,
            req.excluir_inativo_semanas, req.cap_outlier_multiplo,
        )

        for item in projecao_dia.itens:
            writer.writerow([
                item.data,
                item.dia_semana,
                item.cliente,
                item.produto,
                formatar_decimal_br(item.quantidade_projetada),
                item.quantidade_arredondada,
                item.semanas_com_dados,
                f"{item.pct_presenca}%",
                item.semanas_com_produto,
                f"{item.pct_presenca_produto}%",
                "Sim" if item.is_cliente_novo else "Não",
                "Sim" if item.outlier_capado else "Não",
            ])

    output.seek(0)

    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8-sig")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=previsao_vendas_{date.today().isoformat()}.csv"}
    )


@app.get("/clientes")
async def listar_clientes():
    supabase = get_supabase()
    clientes = buscar_todos_clientes(supabase)
    return {"clientes": clientes, "total": len(clientes)}


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": "1.5.0",
        "empresa": EMPRESA,
        "defaults": {
            "presenca_minima_cliente": PRESENCA_MINIMA_CLIENTE_DEFAULT,
            "presenca_minima_produto": PRESENCA_MINIMA_PRODUTO_DEFAULT,
            "excluir_inativo_semanas": EXCLUIR_INATIVO_SEMANAS_DEFAULT,
            "cap_outlier_multiplo": CAP_OUTLIER_MULTIPLO_DEFAULT,
        },
    }


@app.get("/debug/contagem")
async def debug_contagem():
    supabase = get_supabase()
    vendas = buscar_vendas_periodo(supabase, date.today() - timedelta(days=60), date.today())

    datas = {}
    clientes = set()
    for v in vendas:
        d = str(v["data_venda"])[:10]
        datas[d] = datas.get(d, 0) + 1
        clientes.add(v["cliente"])

    return {
        "total_registros": len(vendas),
        "total_clientes_unicos": len(clientes),
        "registros_por_data": dict(sorted(datas.items())),
    }
