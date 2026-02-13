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

app = FastAPI(title="Previsão de Vendas API", version="1.0.0")

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

# Tabela e filtros fixos
TABELA = "vendas_itens_importados"
EMPRESA = "PRATICMIX"

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

class ProjecaoItem(BaseModel):
    data: str
    dia_semana: str
    dia_semana_num: int
    cliente: str
    produto: str
    descricao_item: str
    quantidade_projetada: float
    quantidade_arredondada: int
    semanas_com_dados: int
    is_cliente_novo: bool

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

def media_ponderada(valores: list[float]) -> float:
    if not valores:
        return 0.0
    n = len(valores)
    pesos = list(range(n, 0, -1))
    return sum(v * p for v, p in zip(valores, pesos)) / sum(pesos)


# ─── DATA ACCESS ──────────────────────────────────────────

def buscar_vendas_periodo(supabase, data_inicio: date, data_fim: date) -> list[dict]:
    """
    Busca vendas da PRATICMIX no período.
    Uma query só pra toda a janela de histórico (~40 dias).
    """
    result = supabase.table(TABELA) \
        .select("data_venda, cliente, produto, descricao_item, quantidade") \
        .eq("empresa", EMPRESA) \
        .gte("data_venda", data_inicio.isoformat()) \
        .lte("data_venda", data_fim.isoformat()) \
        .limit(10000) \
        .execute()
    
    return result.data if result.data else []


def buscar_todos_clientes(supabase) -> list[str]:
    """Clientes únicos da PRATICMIX nos últimos 60 dias."""
    data_corte = (date.today() - timedelta(days=60)).isoformat()
    
    result = supabase.table(TABELA) \
        .select("cliente") \
        .eq("empresa", EMPRESA) \
        .gte("data_venda", data_corte) \
        .limit(10000) \
        .execute()
    
    if not result.data:
        return []
    
    return sorted(set(row["cliente"] for row in result.data))


# ─── CORE LOGIC ───────────────────────────────────────────

def calcular_projecao(
    vendas: list[dict],
    data_alvo: date,
    semanas_historico: int,
    clientes_excluidos: list[str],
) -> ProjecaoDia:
    
    datas_historico_str = set(d.isoformat() for d in calcular_datas_historico(data_alvo, semanas_historico))
    
    agrupado = {}
    datas_por_cliente = {}
    
    for venda in vendas:
        data_str = venda["data_venda"]
        if data_str not in datas_historico_str:
            continue
        
        cliente = venda["cliente"]
        if cliente in clientes_excluidos:
            continue
        
        produto = venda["produto"] or ""
        descricao = venda["descricao_item"] or ""
        qtd = float(venda["quantidade"] or 0)
        chave_item = (produto, descricao)
        
        if cliente not in agrupado:
            agrupado[cliente] = {}
            datas_por_cliente[cliente] = set()
        
        datas_por_cliente[cliente].add(data_str)
        
        if chave_item not in agrupado[cliente]:
            agrupado[cliente][chave_item] = []
        
        agrupado[cliente][chave_item].append(qtd)
    
    itens_projecao = []
    dia_semana_nome = get_dia_semana_nome(data_alvo)
    dia_semana_num = data_alvo.weekday()
    
    for cliente, items in agrupado.items():
        semanas_com_dados = len(datas_por_cliente[cliente])
        is_novo = semanas_com_dados < semanas_historico
        
        for (produto, descricao), quantidades in items.items():
            qtd_projetada = media_ponderada(quantidades)
            
            itens_projecao.append(ProjecaoItem(
                data=data_alvo.isoformat(),
                dia_semana=dia_semana_nome,
                dia_semana_num=dia_semana_num,
                cliente=cliente,
                produto=produto,
                descricao_item=descricao,
                quantidade_projetada=round(qtd_projetada, 3),
                quantidade_arredondada=math.ceil(qtd_projetada),
                semanas_com_dados=semanas_com_dados,
                is_cliente_novo=is_novo,
            ))
    
    itens_projecao.sort(key=lambda x: (x.cliente, x.descricao_item))
    total_quantidade = sum(i.quantidade_projetada for i in itens_projecao)
    
    return ProjecaoDia(
        data=data_alvo.isoformat(),
        dia_semana=dia_semana_nome,
        dia_semana_num=dia_semana_num,
        total_itens_projetados=len(itens_projecao),
        total_quantidade=round(total_quantidade, 3),
        itens=itens_projecao,
    )


# ─── ENDPOINTS ────────────────────────────────────────────

@app.post("/projecao", response_model=ProjecaoResponse)
async def gerar_projecao(req: ProjecaoRequest):
    """Projeção detalhada por cliente/item para os próximos N dias."""
    if not 1 <= req.dias_frente <= 7:
        raise HTTPException(400, "dias_frente deve ser entre 1 e 7")
    if not 2 <= req.semanas_historico <= 12:
        raise HTTPException(400, "semanas_historico deve ser entre 2 e 12")
    
    supabase = get_supabase()
    
    data_base = date.fromisoformat(req.data_inicio) if req.data_inicio else date.today() + timedelta(days=1)
    data_mais_antiga = data_base - timedelta(weeks=req.semanas_historico, days=1)
    
    # UMA query só pro período todo
    vendas = buscar_vendas_periodo(supabase, data_mais_antiga, date.today())
    
    dias = []
    todos_clientes = set()
    todos_itens = set()
    
    for i in range(req.dias_frente):
        data_alvo = data_base + timedelta(days=i)
        projecao_dia = calcular_projecao(vendas, data_alvo, req.semanas_historico, req.clientes_excluidos)
        dias.append(projecao_dia)
        
        for item in projecao_dia.itens:
            todos_clientes.add(item.cliente)
            todos_itens.add(item.descricao_item)
    
    return ProjecaoResponse(
        dias=dias,
        total_geral_quantidade=round(sum(d.total_quantidade for d in dias), 3),
        total_clientes=len(todos_clientes),
        total_itens_unicos=len(todos_itens),
        gerado_em=datetime.now().isoformat(),
    )


@app.post("/projecao/consolidado")
async def projecao_consolidada(req: ProjecaoRequest):
    """Projeção consolidada por ITEM (soma de todos os clientes)."""
    if not 1 <= req.dias_frente <= 7:
        raise HTTPException(400, "dias_frente deve ser entre 1 e 7")
    
    supabase = get_supabase()
    
    data_base = date.fromisoformat(req.data_inicio) if req.data_inicio else date.today() + timedelta(days=1)
    data_mais_antiga = data_base - timedelta(weeks=req.semanas_historico, days=1)
    
    vendas = buscar_vendas_periodo(supabase, data_mais_antiga, date.today())
    
    consolidado = []
    
    for i in range(req.dias_frente):
        data_alvo = data_base + timedelta(days=i)
        projecao_dia = calcular_projecao(vendas, data_alvo, req.semanas_historico, req.clientes_excluidos)
        
        itens_consolidados = {}
        for p in projecao_dia.itens:
            chave = (p.produto, p.descricao_item)
            if chave not in itens_consolidados:
                itens_consolidados[chave] = 0.0
            itens_consolidados[chave] += p.quantidade_projetada
        
        for (produto, descricao), qtd in sorted(itens_consolidados.items(), key=lambda x: x[0][1]):
            consolidado.append({
                "data": data_alvo.isoformat(),
                "dia_semana": get_dia_semana_nome(data_alvo),
                "produto": produto,
                "descricao_item": descricao,
                "quantidade_projetada": round(qtd, 3),
                "quantidade_arredondada": math.ceil(qtd),
            })
    
    return consolidado


@app.post("/projecao/download")
async def download_projecao(req: ProjecaoRequest):
    """Download CSV da projeção detalhada."""
    supabase = get_supabase()
    
    data_base = date.fromisoformat(req.data_inicio) if req.data_inicio else date.today() + timedelta(days=1)
    data_mais_antiga = data_base - timedelta(weeks=req.semanas_historico, days=1)
    
    vendas = buscar_vendas_periodo(supabase, data_mais_antiga, date.today())
    
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["DATA", "DIA_SEMANA", "CLIENTE", "PRODUTO", "DESCRICAO_ITEM", "QTD_PROJETADA", "QTD_ARREDONDADA", "SEMANAS_HISTORICO", "CLIENTE_NOVO"])
    
    for i in range(req.dias_frente):
        data_alvo = data_base + timedelta(days=i)
        projecao_dia = calcular_projecao(vendas, data_alvo, req.semanas_historico, req.clientes_excluidos)
        
        for item in projecao_dia.itens:
            writer.writerow([
                item.data,
                item.dia_semana,
                item.cliente,
                item.produto,
                item.descricao_item,
                item.quantidade_projetada,
                item.quantidade_arredondada,
                item.semanas_com_dados,
                "Sim" if item.is_cliente_novo else "Não",
            ])
    
    output.seek(0)
    
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8-sig")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=previsao_vendas_{date.today().isoformat()}.csv"}
    )


@app.get("/clientes")
async def listar_clientes():
    """Lista clientes únicos da PRATICMIX (últimos 60 dias)."""
    supabase = get_supabase()
    return buscar_todos_clientes(supabase)


@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0", "empresa": EMPRESA}
