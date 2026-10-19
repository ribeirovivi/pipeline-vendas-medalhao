"""
gerar_dados_exemplo.py
-----------------------------------------------------------------------------
Gera os dados brutos de exemplo do projeto "Pipeline de Ingestão e
Transformação de Vendas". Simula uma loja fictícia de artigos esportivos
("Vitrine Esportiva") que vende por 3 canais diferentes, cada um exportando
os dados em um formato distinto — exatamente o cenário de "dados dispersos
em múltiplas fontes, sem padronização" descrito no problema do projeto.

Rode este script UMA VEZ para gerar os arquivos brutos em data/raw/. Ele usa
uma seed fixa (42), então sempre gera os mesmos dados — isso é proposital,
para que você consiga reproduzir os mesmos resultados sempre que rodar o
pipeline e comparar antes/depois.

Uso:
    python scripts/gerar_dados_exemplo.py
"""

import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
REF_DIR = BASE_DIR / "data" / "referencia"

# -----------------------------------------------------------------------
# Catálogo de produtos (tabela de referência / dimensão)
# -----------------------------------------------------------------------
# `sku` é o identificador canônico, usado pela loja online e pelo
# marketplace. A loja física, por ser um sistema de PDV mais antigo, usa um
# "código legado" próprio — isso simula um problema real de integração de
# dados: sistemas diferentes identificam o mesmo produto de formas
# diferentes, e é preciso uma tabela de referência para unificar.
PRODUTOS = [
    # sku,        nome,                          categoria,     marca,       codigo_legado_loja_fisica
    ("SKU-0001", "Tênis Runner Pro X",           "Calçados",    "Vertex",    "COD-101"),
    ("SKU-0002", "Tênis Trail Grip",             "Calçados",    "Vertex",    "COD-102"),
    ("SKU-0003", "Camiseta Dry-Fit Masculina",   "Vestuário",   "Aerofit",   "COD-201"),
    ("SKU-0004", "Camiseta Dry-Fit Feminina",    "Vestuário",   "Aerofit",   "COD-202"),
    ("SKU-0005", "Shorts de Corrida",            "Vestuário",   "Aerofit",   "COD-203"),
    ("SKU-0006", "Bola de Futebol Campo",        "Bolas",       "Chuteira10","COD-301"),
    ("SKU-0007", "Bola de Vôlei Oficial",        "Bolas",       "Chuteira10","COD-302"),
    ("SKU-0008", "Luva de Goleiro Grip",         "Acessórios",  "Chuteira10","COD-303"),
    ("SKU-0009", "Squeeze Térmico 1L",           "Acessórios",  "Hydra",     "COD-401"),
    ("SKU-0010", "Mochila Esportiva 25L",        "Acessórios",  "Hydra",     "COD-402"),
    ("SKU-0011", "Faixa de Resistência Kit",     "Fitness",     "Corebase",  "COD-501"),
    ("SKU-0012", "Colchonete de Yoga",           "Fitness",     "Corebase",  "COD-502"),
    ("SKU-0013", "Corda de Pular Profissional",  "Fitness",     "Corebase",  "COD-503"),
    ("SKU-0014", "Meião de Compressão",          "Vestuário",   "Aerofit",   "COD-204"),
    ("SKU-0015", "Boné Esportivo UV",            "Acessórios",  "Hydra",     "COD-403"),
]

PRECOS_BASE = {
    "SKU-0001": 349.90, "SKU-0002": 399.90, "SKU-0003": 79.90, "SKU-0004": 79.90,
    "SKU-0005": 99.90, "SKU-0006": 129.90, "SKU-0007": 149.90, "SKU-0008": 189.90,
    "SKU-0009": 89.90, "SKU-0010": 219.90, "SKU-0011": 69.90, "SKU-0012": 119.90,
    "SKU-0013": 49.90, "SKU-0014": 39.90, "SKU-0015": 59.90
}

LOJAS_FISICAS = ["Loja SP - Paulista", "Loja RJ - Barra", "Loja BH - Savassi"]
MARKETPLACES = ["ShopFast", "MegaVarejo"]


def preco_com_variacao(sku, variacao=0.05):
    base = PRECOS_BASE[sku]
    fator = 1 + random.uniform(-variacao, variacao)
    return round(base * fator, 2)


def gerar_dim_produto():
    REF_DIR.mkdir(parents=True, exist_ok=True)
    caminho = REF_DIR / "dim_produto.csv"
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["sku", "nome_produto", "categoria", "marca", "codigo_legado_loja_fisica"])
        writer.writerows(PRODUTOS)
    print(f"[ok] {caminho}")


def datas_do_mes(ano, mes, quantidade):
    primeiro_dia = date(ano, mes, 1)
    dias_no_mes = 28 if mes == 2 else 30
    return [primeiro_dia + timedelta(days=random.randint(0, dias_no_mes - 1)) for _ in range(quantidade)]


# -----------------------------------------------------------------------
# Fonte 1: Loja Online (E-commerce) — CSV, vírgula, datas ISO
# -----------------------------------------------------------------------
def gerar_loja_online(ano, mes, n_pedidos, id_inicial):
    pasta = RAW_DIR / "loja_online"
    pasta.mkdir(parents=True, exist_ok=True)
    nome_arquivo = f"vendas_online_{ano}-{mes:02d}.csv"
    caminho = pasta / nome_arquivo

    linhas = []
    datas = datas_do_mes(ano, mes, n_pedidos)
    for i in range(n_pedidos):
        sku = random.choice(PRODUTOS)[0]
        linhas.append({
            "id_pedido": f"ON-{id_inicial + i:06d}",
            "data_pedido": datas[i].isoformat(),
            "email_cliente": f"cliente{random.randint(1000, 9999)}@exemplo.com",
            "sku_produto": sku,
            "quantidade": random.randint(1, 3),
            "valor_unitario": preco_com_variacao(sku),
            "canal": random.choice(["site", "app"])
        })

    # --- Injeção proposital de "sujeira" para o pipeline tratar ---
    # 1) Registro duplicado exato (simula reenvio do sistema de origem)
    linhas.append(dict(linhas[0]))
    # 2) Registro com valor_unitario ausente (erro de exportação)
    linha_invalida = dict(linhas[2])
    linha_invalida["id_pedido"] = f"ON-{id_inicial + n_pedidos:06d}"
    linha_invalida["valor_unitario"] = ""
    linhas.append(linha_invalida)

    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(linhas[0].keys()))
        writer.writeheader()
        writer.writerows(linhas)
    print(f"[ok] {caminho} ({len(linhas)} linhas)")


# -----------------------------------------------------------------------
# Fonte 2: Loja Física (PDV) — CSV, ponto-e-vírgula, decimal com vírgula,
# datas BR, cabeçalhos abreviados, código de produto LEGADO (não é o SKU)
# -----------------------------------------------------------------------
def gerar_loja_fisica(ano, mes, n_vendas, id_inicial):
    pasta = RAW_DIR / "loja_fisica"
    pasta.mkdir(parents=True, exist_ok=True)
    nome_arquivo = f"vendas_fisica_{ano}-{mes:02d}.csv"
    caminho = pasta / nome_arquivo

    linhas = []
    datas = datas_do_mes(ano, mes, n_vendas)
    for i in range(n_vendas):
        produto = random.choice(PRODUTOS)
        sku, codigo_legado = produto[0], produto[4]
        preco = preco_com_variacao(sku)
        linhas.append({
            "COD_VENDA": f"FIS-{id_inicial + i:06d}",
            "DT_VENDA": datas[i].strftime("%d/%m/%Y"),
            "COD_PRODUTO": codigo_legado,
            "QTD": random.randint(1, 4),
            "VL_UNIT": f"{preco:.2f}".replace(".", ","),
            "LOJA": random.choice(LOJAS_FISICAS)
        })

    # --- Sujeira proposital ---
    # 1) Quantidade inválida (erro de digitação no PDV)
    linha_invalida = dict(linhas[1])
    linha_invalida["COD_VENDA"] = f"FIS-{id_inicial + n_vendas:06d}"
    linha_invalida["QTD"] = "-1"
    linhas.append(linha_invalida)
    # 2) Código de produto que não existe na tabela de referência
    # (ex: produto descontinuado, cadastro incompleto no PDV)
    linha_orfa = dict(linhas[2])
    linha_orfa["COD_VENDA"] = f"FIS-{id_inicial + n_vendas + 1:06d}"
    linha_orfa["COD_PRODUTO"] = "COD-999"
    linhas.append(linha_orfa)

    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(linhas[0].keys()), delimiter=";")
        writer.writeheader()
        writer.writerows(linhas)
    print(f"[ok] {caminho} ({len(linhas)} linhas)")


# -----------------------------------------------------------------------
# Fonte 3: Marketplace — JSON Lines, timestamp Unix, pedido com múltiplos
# itens aninhados (schema totalmente diferente das outras duas fontes)
# -----------------------------------------------------------------------
def gerar_marketplace(ano, mes, n_pedidos, id_inicial):
    import calendar

    pasta = RAW_DIR / "marketplace"
    pasta.mkdir(parents=True, exist_ok=True)
    nome_arquivo = f"pedidos_marketplace_{ano}-{mes:02d}.jsonl"
    caminho = pasta / nome_arquivo

    dias_no_mes = calendar.monthrange(ano, mes)[1]

    pedidos = []
    for i in range(n_pedidos):
        dia = random.randint(1, dias_no_mes)
        timestamp = int(date(ano, mes, dia).strftime("%s")) if hasattr(date, "strftime") else 0
        # calendar.timegm garante consistência independente de timezone
        import time
        timestamp = calendar.timegm(date(ano, mes, dia).timetuple())

        n_itens = random.randint(1, 3)
        skus_pedido = random.sample([p[0] for p in PRODUTOS], k=n_itens)
        itens = [
            {"sku": sku, "qty": random.randint(1, 2), "price": preco_com_variacao(sku)}
            for sku in skus_pedido
        ]
        pedidos.append({
            "orderId": f"MKT-{id_inicial + i:06d}",
            "timestamp": timestamp,
            "marketplace": random.choice(MARKETPLACES),
            "items": itens
        })

    # --- Sujeira proposital: item com SKU que não existe na referência ---
    pedido_invalido = {
        "orderId": f"MKT-{id_inicial + n_pedidos:06d}",
        "timestamp": calendar.timegm(date(ano, mes, 10).timetuple()),
        "marketplace": "ShopFast",
        "items": [{"sku": "SKU-9999", "qty": 1, "price": 99.90}]
    }
    pedidos.append(pedido_invalido)

    with open(caminho, "w", encoding="utf-8") as f:
        for pedido in pedidos:
            f.write(json.dumps(pedido, ensure_ascii=False) + "\n")
    print(f"[ok] {caminho} ({len(pedidos)} pedidos)")


if __name__ == "__main__":
    gerar_dim_produto()

    # Run 1 — carga inicial (Janeiro/2026)
    gerar_loja_online(2026, 1, n_pedidos=120, id_inicial=1)
    gerar_loja_fisica(2026, 1, n_vendas=90, id_inicial=1)
    gerar_marketplace(2026, 1, n_pedidos=60, id_inicial=1)

    # Run 2 — carga incremental (Fevereiro/2026), simula "chegada de dados
    # novos" para demonstrar que o pipeline processa só o que é novo
    gerar_loja_online(2026, 2, n_pedidos=140, id_inicial=1000)
    gerar_loja_fisica(2026, 2, n_vendas=100, id_inicial=1000)
    gerar_marketplace(2026, 2, n_pedidos=70, id_inicial=1000)

    print("\nDados de exemplo gerados com sucesso em data/raw/ e data/referencia/.")
