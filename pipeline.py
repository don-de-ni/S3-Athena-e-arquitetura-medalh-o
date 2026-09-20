from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd


# ==================================================
# 1. CONFIGURAÇÃO DOS CAMINHOS
# ==================================================

PASTA_PROJETO = Path(__file__).resolve().parent
PASTA_DADOS = PASTA_PROJETO / "dados"

# Altere esta data quando gerar outra base.
DATA_INGESTAO = "2026-09-20"
PARTICAO = f"ingest_date={DATA_INGESTAO}"

CAMINHO_CLIENTES = (
    PASTA_DADOS / "raw" / "clientes" / PARTICAO / "clientes.csv"
)

CAMINHO_PRODUTOS = (
    PASTA_DADOS / "raw" / "produtos" / PARTICAO / "produtos.csv"
)

CAMINHO_PEDIDOS = (
    PASTA_DADOS / "raw" / "pedidos" / PARTICAO / "pedidos.csv"
)

PASTA_QUARENTENA = (
    PASTA_DADOS
    / "quarantine"
    / "pedidos_rejeitados"
    / f"data={DATA_INGESTAO}"
)

PASTA_SILVER = (
    PASTA_DADOS / "processed" / "fato_vendas" / PARTICAO
)

PASTA_GOLD = (
    PASTA_DADOS / "gold" / "vendas_uf_categoria" / PARTICAO
)

for pasta in [PASTA_QUARENTENA, PASTA_SILVER, PASTA_GOLD]:
    pasta.mkdir(parents=True, exist_ok=True)


# ==================================================
# 2. LEITURA DOS CSVs
# ==================================================

for caminho in [
    CAMINHO_CLIENTES,
    CAMINHO_PRODUTOS,
    CAMINHO_PEDIDOS,
]:
    if not caminho.is_file():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {caminho}"
        )

clientes = pd.read_csv(CAMINHO_CLIENTES)

produtos = pd.read_csv(
    CAMINHO_PRODUTOS,
    dtype={"preco": str},
)

pedidos = pd.read_csv(CAMINHO_PEDIDOS)

# Usa Decimal para evitar imprecisões monetárias.
produtos["preco"] = produtos["preco"].map(Decimal)

print(f"\nPartição carregada: {PARTICAO}")
print(f"Clientes: {len(clientes)}")
print(f"Produtos: {len(produtos)}")
print(f"Pedidos: {len(pedidos)}")


# ==================================================
# 3. VERIFICAÇÃO DE DUPLICIDADES
# ==================================================

# Nesta base, cada pedido tem apenas um produto
# e deve aparecer uma única vez.
if pedidos["pedido_id"].duplicated().any():
    raise ValueError("Existem pedidos com pedido_id duplicado.")

if clientes["cliente_id"].duplicated().any():
    raise ValueError("Existem clientes com cliente_id duplicado.")

if produtos["product_id"].duplicated().any():
    raise ValueError("Existem produtos com product_id duplicado.")


# ==================================================
# 4. DATA QUALITY
# ==================================================

clientes_existentes = set(clientes["cliente_id"])
produtos_existentes = set(produtos["product_id"])


def verificar_pedido(pedido):
    motivos = []

    if pedido["quantidade"] <= 0:
        motivos.append("QUANTIDADE_NAO_POSITIVA")

    if pedido["cliente_id"] not in clientes_existentes:
        motivos.append("CLIENTE_INEXISTENTE")

    if pedido["product_id"] not in produtos_existentes:
        motivos.append("PRODUTO_INEXISTENTE")

    return motivos


pedidos["motivos_rejeicao"] = pedidos.apply(
    verificar_pedido,
    axis=1,
)

# Lista vazia de motivos significa que o pedido é válido.
pedido_valido = pedidos["motivos_rejeicao"].map(len) == 0

validos = (
    pedidos.loc[pedido_valido]
    .drop(columns="motivos_rejeicao")
    .copy()
)

rejeitados = pedidos.loc[~pedido_valido].copy()


# ==================================================
# 5. SILVER: ENRIQUECIMENTO E VALOR TOTAL
# ==================================================

silver = (
    validos
    .merge(
        clientes,
        on="cliente_id",
        how="inner",
        validate="many_to_one",
    )
    .merge(
        produtos,
        on="product_id",
        how="inner",
        validate="many_to_one",
    )
    .rename(columns={"nome": "nome_cliente"})
)

# Converte a data do pedido para um tipo de data.
silver["data_pedido"] = pd.to_datetime(
    silver["data_pedido"],
    format="%Y-%m-%d",
).dt.date

silver["valor_total"] = [
    (Decimal(int(quantidade)) * preco).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )
    for quantidade, preco in zip(
        silver["quantidade"],
        silver["preco"],
    )
]


# ==================================================
# 6. GOLD: AGREGAÇÃO POR UF E CATEGORIA
# ==================================================

gold = (
    silver.groupby(["uf", "categoria"], as_index=False)
    .agg(
        quantidade_pedidos=("pedido_id", "count"),
        unidades_vendidas=("quantidade", "sum"),
        faturamento=("valor_total", "sum"),
    )
)

gold["ticket_medio"] = [
    (faturamento / Decimal(int(quantidade))).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )
    for faturamento, quantidade in zip(
        gold["faturamento"],
        gold["quantidade_pedidos"],
    )
]


# ==================================================
# 7. CONCILIAÇÃO ANTES DE GRAVAR
# ==================================================

ids_raw = set(pedidos["pedido_id"])
ids_silver = set(silver["pedido_id"])
ids_quarentena = set(rejeitados["pedido_id"])

contagem_ok = (
    len(pedidos) == len(silver) + len(rejeitados)
)

sem_sobreposicao = ids_silver.isdisjoint(ids_quarentena)

todos_classificados = (
    ids_raw == (ids_silver | ids_quarentena)
)

faturamento_ok = (
    silver["valor_total"].sum()
    == gold["faturamento"].sum()
)

quantidade_pedidos_ok = (
    len(silver) == gold["quantidade_pedidos"].sum()
)

unidades_ok = (
    silver["quantidade"].sum()
    == gold["unidades_vendidas"].sum()
)

verificacoes = {
    "Raw = Silver + Quarentena": contagem_ok,
    "Silver e quarentena sem IDs em comum": sem_sobreposicao,
    "Todos os pedidos foram classificados": todos_classificados,
    "Faturamento Silver = Gold": faturamento_ok,
    "Quantidade de pedidos Silver = Gold": quantidade_pedidos_ok,
    "Unidades vendidas Silver = Gold": unidades_ok,
}

for descricao, resultado in verificacoes.items():
    if not resultado:
        raise ValueError(f"Falha na conciliação: {descricao}")


# ==================================================
# 8. GRAVAÇÃO DOS RESULTADOS
# ==================================================

arquivo_quarentena = PASTA_QUARENTENA / "rejeitados.json"
arquivo_silver = PASTA_SILVER / "fato_vendas.parquet"
arquivo_gold = PASTA_GOLD / "vendas_uf_categoria.parquet"

# JSON Lines: um registro rejeitado por linha,
# contendo todos os motivos encontrados.
rejeitados.to_json(
    arquivo_quarentena,
    orient="records",
    lines=True,
    force_ascii=False,
)

silver.to_parquet(
    arquivo_silver,
    engine="pyarrow",
    compression="snappy",
    index=False,
)

gold.to_parquet(
    arquivo_gold,
    engine="pyarrow",
    compression="snappy",
    index=False,
)


# ==================================================
# 9. RESUMO DA EXECUÇÃO
# ==================================================

print("\nPipeline concluído!")
print(f"Pedidos Raw: {len(pedidos)}")
print(f"Pedidos Silver: {len(silver)}")
print(f"Pedidos em quarentena: {len(rejeitados)}")

print("\nConciliação:")
for descricao, resultado in verificacoes.items():
    print(f"  {descricao}: {'OK' if resultado else 'FALHOU'}")

print("\nRejeições por motivo:")
contagem_motivos = (
    rejeitados["motivos_rejeicao"]
    .explode()
    .value_counts()
)

if contagem_motivos.empty:
    print("  Nenhum pedido rejeitado.")
else:
    print(contagem_motivos.to_string())

print("\nResultado da Gold:")
print(gold.to_string(index=False))

print("\nArquivos gerados:")
print(f"Quarentena: {arquivo_quarentena}")
print(f"Silver: {arquivo_silver}")
print(f"Gold: {arquivo_gold}")