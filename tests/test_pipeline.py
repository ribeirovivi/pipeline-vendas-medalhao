"""
Testes automatizados do pipeline de vendas (medalhão).

Como rodar (na raiz do projeto, com o ambiente do pipeline ativo):

    python -m unittest discover -s tests -v

Pré-requisitos: os mesmos do pipeline (Java 11/17 e `pip install -r requirements.txt`).
No Windows, defina HADOOP_HOME apontando para uma pasta com bin\\winutils.exe.

O que é testado
  1. Gate de qualidade: cada regra manda o registro para a quarentena.
  2. Deduplicação determinística: mantém a versão mais recente, sempre.
  3. Ponta a ponta (processo separado, em pasta temporária):
       - 1ª execução ingere as 3 fontes e a reconciliação fecha;
       - 2ª execução não reprocessa nada (carga incremental/idempotência);
       - arquivo novo repetido é ingerido, mas as duplicatas são contadas
         e as linhas válidas não mudam.
"""
import csv
import datetime as dt
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
SRC = RAIZ / "src"
sys.path.insert(0, str(SRC))

import transform_silver as ts  # noqa: E402
from spark_session import get_spark  # noqa: E402


class TestRegrasDeQualidade(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("testes-pipeline")

    @staticmethod
    def _lit(v):
        if v is None:
            return "NULL"
        if isinstance(v, dt.date):
            return f"DATE'{v.isoformat()}'"
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        return repr(v)

    def _df(self, linhas):
        """Monta um DataFrame via SQL VALUES (usa só a JVM; evita workers Python)."""
        valores = ",".join("(" + ",".join(self._lit(v) for v in l) + ")" for l in linhas)
        return self.spark.sql(
            "SELECT * FROM VALUES " + valores +
            " AS t(id_transacao, data, sku, categoria, quantidade, preco_unitario, fonte, _ingerido_em)")

    def test_cada_regra_envia_para_quarentena(self):
        d = dt.date(2026, 1, 10)
        linhas = [
            ("T1", d, "SKU-1", "Bolas", 2, 10.0, "loja_online", "2026-01-10T10:00:00"),   # válido
            ("T2", None, "SKU-1", "Bolas", 2, 10.0, "loja_online", "2026-01-10T10:00:00"),  # data inválida
            ("T3", d, "SKU-1", "Bolas", 2, None, "loja_online", "2026-01-10T10:00:00"),   # preço nulo
            ("T4", d, "SKU-X", None, 2, 10.0, "loja_online", "2026-01-10T10:00:00"),      # fora da referência
            ("T5", d, "SKU-1", "Bolas", 0, 10.0, "loja_online", "2026-01-10T10:00:00"),   # quantidade 0
            ("T6", d, "SKU-1", "Bolas", None, 10.0, "loja_online", "2026-01-10T10:00:00"),  # quantidade nula
        ]
        validos, rejeitados = ts._aplicar_gate_qualidade(self._df(linhas))
        self.assertEqual([r.id_transacao for r in validos.collect()], ["T1"])
        motivos = {r.id_transacao: r.motivo_rejeicao for r in rejeitados.collect()}
        self.assertEqual(motivos, {
            "T2": "data_invalida", "T3": "preco_unitario_nulo",
            "T4": "produto_nao_encontrado_na_referencia",
            "T5": "quantidade_invalida", "T6": "quantidade_invalida"})

    def test_deduplicacao_mantem_a_versao_mais_recente_e_e_deterministica(self):
        d = dt.date(2026, 1, 10)
        linhas = [
            ("T1", d, "SKU-1", "Bolas", 1, 10.0, "loja_online", "2026-01-10T08:00:00"),
            ("T1", d, "SKU-1", "Bolas", 5, 10.0, "loja_online", "2026-01-11T08:00:00"),  # mais recente
            ("T2", d, "SKU-1", "Bolas", 1, 10.0, "loja_online", "2026-01-10T08:00:00"),
        ]
        resultados = []
        for _ in range(3):
            df = ts._deduplicar(self._df(linhas))
            resultados.append(sorted((r.id_transacao, r.quantidade) for r in df.collect()))
        self.assertEqual(resultados[0], [("T1", 5), ("T2", 1)])
        self.assertTrue(all(r == resultados[0] for r in resultados))


class TestPontaAPonta(unittest.TestCase):
    """Executa o orquestrador em um processo separado, numa pasta temporária."""

    def _rodar(self, base):
        env = dict(os.environ, PIPELINE_BASE_DIR=str(base), PYSPARK_PYTHON=sys.executable)
        r = subprocess.run([sys.executable, "orchestrator.py"], cwd=SRC, env=env,
                           capture_output=True, text=True, timeout=900)
        self.assertEqual(r.returncode, 0, msg=r.stdout[-1500:] + r.stderr[-1500:])

    @staticmethod
    def _log(base):
        with open(Path(base) / "control" / "pipeline_execution_log.csv", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    def test_incremental_idempotente_e_reconciliacao(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shutil.copytree(RAIZ / "data" / "raw", base / "data" / "raw")
            shutil.copytree(RAIZ / "data" / "referencia", base / "data" / "referencia")

            # 1ª execução: ingere as 3 fontes
            self._rodar(base)
            log = self._log(base)
            bronze = [l for l in log if l["camada"] == "bronze"]
            self.assertEqual([l["status"] for l in bronze], ["sucesso"] * 3)
            silver1 = [l for l in log if l["camada"] == "silver"][-1]
            lidas, validas = int(silver1["linhas_lidas"]), int(silver1["linhas_gravadas"])
            rej, dup = int(silver1["linhas_rejeitadas"]), int(silver1["linhas_duplicadas"])
            self.assertEqual(lidas, validas + rej + dup)          # reconciliação

            # 2ª execução: nada novo
            n_antes = len(log)
            self._rodar(base)
            novo = self._log(base)[n_antes:]
            self.assertEqual([l["status"] for l in novo if l["camada"] == "bronze"],
                             ["sem_novidade"] * 3)
            silver2 = [l for l in novo if l["camada"] == "silver"][-1]
            self.assertEqual(int(silver2["linhas_gravadas"]), validas)

            # arquivo novo, idêntico a um existente: ingere 1 arquivo; duplicatas contadas
            origem = base / "data" / "raw" / "loja_online" / "vendas_online_2026-02.csv"
            shutil.copy(origem, origem.with_name("vendas_online_2026-03.csv"))
            n_antes = len(self._log(base))
            self._rodar(base)
            novo = self._log(base)[n_antes:]
            bronze3 = {l["fonte"]: l["status"] for l in novo if l["camada"] == "bronze"}
            self.assertEqual(bronze3, {"loja_online": "sucesso", "loja_fisica": "sem_novidade",
                                       "marketplace": "sem_novidade"})
            silver3 = [l for l in novo if l["camada"] == "silver"][-1]
            self.assertEqual(int(silver3["linhas_gravadas"]), validas)       # válidas não mudam
            self.assertGreater(int(silver3["linhas_duplicadas"]), dup)       # duplicatas contadas
            self.assertEqual(int(silver3["linhas_lidas"]),
                             int(silver3["linhas_gravadas"]) + int(silver3["linhas_rejeitadas"])
                             + int(silver3["linhas_duplicadas"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
