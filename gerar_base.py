import argparse
import csv
import json
import random

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path


def escrever_csv(caminho, colunas, registros):
    caminho.parent.mkdir(parents=True, exist_ok=True)

    with caminho.open("w", encoding="utf-8", newline="") as arquivo:
        writer = csv.DictWriter(
            arquivo,
            fieldnames=colunas,
            delimiter=",",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(registros)


def gerar_base(data_ingestao, diretorio_saida):
    # Semente fixa: mesma data + mesmo código = mesma base.
    rng = random.Random(42)

    raiz = Path(diretorio_saida)
    particao = f"ingest_date={data_ingestao.isoformat()}"

    # -------------------------
    # Dimensão de clientes
    # -------------------------
    ufs = ["SP", "RJ", "MG", "PR", "BA"]

    clientes = [
        {
            "cliente_id": cliente_id,
            "nome": f"Cliente Teste {cliente_id:03d}",
            "email": f"cliente{cliente_id:03d}@example.com",
            "uf": ufs[(cliente_id - 1) % len(ufs)],
        }
        for cliente_id in range(1, 21)
    ]

    # -------------------------
    # Dimensão de produtos
    # -------------------------
    catalogo = [
        ("Mouse sem fio", "Eletronicos", "89.90"),
        ("Teclado USB", "Eletronicos", "129.90"),
        ("Monitor 24 polegadas", "Eletronicos", "899.90"),
        ("Fone de ouvido", "Eletronicos", "159.90"),
        ("Camiseta basica", "Vestuario", "49.90"),
        ("Calca jeans", "Vestuario", "139.90"),
        ("Jaqueta", "Vestuario", "219.90"),
        ("Tenis casual", "Vestuario", "199.90"),
        ("Garrafa termica", "Casa", "79.90"),
        ("Luminaria de mesa", "Casa", "119.90"),
        ("Organizador", "Casa", "39.90"),
        ("Jogo de toalhas", "Casa", "99.90"),
    ]

    produtos = [
        {
            "product_id": product_id,
            "nome_produto": nome,
            "categoria": categoria,
            "preco": preco,
        }
        for product_id, (nome, categoria, preco)
        in enumerate(catalogo, start=1)
    ]

    # -------------------------
    # Pedidos e erros planejados
    # -------------------------
    pedidos = []
    anomalias_esperadas = []

    for pedido_id in range(1, 101):
        pedido = {
            "pedido_id": pedido_id,
            "cliente_id": rng.randint(1, len(clientes)),
            "product_id": rng.randint(1, len(produtos)),
            "quantidade": rng.randint(1, 5),
            "data_pedido": (
                data_ingestao - timedelta(days=rng.randint(0, 6))
            ).isoformat(),
        }

        motivos = []

        if 81 <= pedido_id <= 85:
            pedido["quantidade"] = 0
            motivos = ["QUANTIDADE_NAO_POSITIVA"]

        elif 86 <= pedido_id <= 90:
            pedido["quantidade"] = -rng.randint(1, 5)
            motivos = ["QUANTIDADE_NAO_POSITIVA"]

        elif 91 <= pedido_id <= 94:
            pedido["cliente_id"] = 9999
            motivos = ["CLIENTE_INEXISTENTE"]

        elif 95 <= pedido_id <= 98:
            pedido["product_id"] = 9999
            motivos = ["PRODUTO_INEXISTENTE"]

        elif 99 <= pedido_id <= 100:
            pedido["quantidade"] = -2
            pedido["cliente_id"] = 9999
            pedido["product_id"] = 9999
            motivos = [
                "QUANTIDADE_NAO_POSITIVA",
                "CLIENTE_INEXISTENTE",
                "PRODUTO_INEXISTENTE",
            ]

        pedidos.append(pedido)

        if motivos:
            anomalias_esperadas.append({
                "pedido_id": pedido_id,
                "motivos": motivos,
            })

    # -------------------------
    # Persistência da camada Raw
    # -------------------------
    escrever_csv(
        raiz / "raw" / "clientes" / particao / "clientes.csv",
        ["cliente_id", "nome", "email", "uf"],
        clientes,
    )

    escrever_csv(
        raiz / "raw" / "produtos" / particao / "produtos.csv",
        ["product_id", "nome_produto", "categoria", "preco"],
        produtos,
    )

    escrever_csv(
        raiz / "raw" / "pedidos" / particao / "pedidos.csv",
        [
            "pedido_id",
            "cliente_id",
            "product_id",
            "quantidade",
            "data_pedido",
        ],
        pedidos,
    )

    # -------------------------
    # Gabarito para testes
    # Não é a saída do pipeline.
    # -------------------------
    ids_invalidos = {
        registro["pedido_id"]
        for registro in anomalias_esperadas
    }

    pedidos_validos = [
        pedido
        for pedido in pedidos
        if pedido["pedido_id"] not in ids_invalidos
    ]

    precos = {
        produto["product_id"]: Decimal(produto["preco"])
        for produto in produtos
    }

    valor_total_esperado = sum(
        (
            Decimal(pedido["quantidade"])
            * precos[pedido["product_id"]]
            for pedido in pedidos_validos
        ),
        Decimal("0.00"),
    )

    resumo = {
        "ingest_date": data_ingestao.isoformat(),
        "clientes_raw": len(clientes),
        "produtos_raw": len(produtos),
        "pedidos_raw": len(pedidos),
        "pedidos_validos_esperados": len(pedidos_validos),
        "pedidos_rejeitados_esperados": len(ids_invalidos),
        "unidades_vendidas_esperadas": sum(
            pedido["quantidade"]
            for pedido in pedidos_validos
        ),
        "valor_total_vendas_esperado": str(
            valor_total_esperado.quantize(Decimal("0.01"))
        ),
        "anomalias_esperadas": anomalias_esperadas,
    }

    # Fora de raw/: evita misturar JSON com os CSVs das tabelas.
    caminho_resumo = (
        raiz
        / "controle"
        / particao
        / "resultado_esperado.json"
    )
    caminho_resumo.parent.mkdir(parents=True, exist_ok=True)
    caminho_resumo.write_text(
        json.dumps(resumo, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Base criada em: {raiz.resolve()}")
    print(f"Partição: {particao}")
    print(f"Clientes: {len(clientes)}")
    print(f"Produtos: {len(produtos)}")
    print(f"Pedidos: {len(pedidos)}")
    print(f"Válidos esperados: {len(pedidos_validos)}")
    print(f"Rejeitados esperados: {len(ids_invalidos)}")
    print(f"Valor total esperado: R$ {valor_total_esperado:.2f}")
    print(f"Gabarito: {caminho_resumo}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gera uma base simulada para um pipeline Medallion."
    )
    parser.add_argument(
        "--data",
        type=date.fromisoformat,
        default=date.today(),
        help="Data de ingestão no formato YYYY-MM-DD.",
    )
    parser.add_argument(
        "--saida",
        default="dados",
        help="Diretório local de saída.",
    )

    args = parser.parse_args()
    gerar_base(args.data, args.saida)