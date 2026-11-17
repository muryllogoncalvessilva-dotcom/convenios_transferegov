"""
Opcional: escreve a saída de data/ numa planilha Google, só em abas próprias (prefixo TG_).

Por que abas separadas: a planilha de controle do setor tem abas e fórmulas mantidas por pessoas.
Este script nunca escreve nelas. Cada tabela vai inteira para a aba TG_<nome> (limpa e regrava),
e o resumo vai para TG_RESUMO. Quem quiser cruzar com as abas manuais usa fórmula (VLOOKUP/XLOOKUP) apontando para TG_*.

Variáveis de ambiente (no GitHub Actions ficam em Secrets):
  GOOGLE_SA_JSON   conteúdo do JSON da conta de serviço (compartilhe a planilha com o e-mail dela, como editor)
  PLANILHA_ID      o id entre /d/ e /edit na URL da planilha
Sem as duas, o script apenas avisa e sai sem erro: o pipeline continua funcionando só com o git.
"""
import json
import os
import sys

import pandas as pd

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PASTA_DADOS = os.path.join(RAIZ, "data")
ABAS = ["resumo", "convenio", "proposta", "historico_situacao", "termo_aditivo", "prorroga_oficio",
        "desbloqueio_cr", "desembolso", "empenho", "cronograma_desembolso", "acomp_obras_contratos_medicoes_modulo_empresas", "licitacao", "contrato"]
NOME_ABA = {"acomp_obras_contratos_medicoes_modulo_empresas": "TG_MEDICOES"}


def nome_aba(tabela):
    return NOME_ABA.get(tabela, "TG_" + tabela.upper())


def valores_para_planilha(df):
    """Cabeçalho + linhas, tudo como texto; None/NaN vira vazio."""
    df = df.fillna("")
    return [list(df.columns)] + df.astype(str).values.tolist()


def enviar(pasta=PASTA_DADOS):
    sa = os.environ.get("GOOGLE_SA_JSON", "").strip()
    pid = os.environ.get("PLANILHA_ID", "").strip()
    if not sa or not pid:
        print("Planilha: GOOGLE_SA_JSON/PLANILHA_ID não definidos; pulando (a saída fica só em data/).")
        return 0
    import gspread  # só importa quando vai usar
    gc = gspread.service_account_from_dict(json.loads(sa))
    ss = gc.open_by_key(pid)
    existentes = {w.title: w for w in ss.worksheets()}
    for tabela in ABAS:
        caminho = os.path.join(pasta, tabela + ".csv")
        if not os.path.exists(caminho):
            continue
        df = pd.read_csv(caminho, sep=";", dtype=str, encoding="utf-8-sig", keep_default_na=False)
        aba = nome_aba(tabela)
        if not aba.startswith("TG_"):
            raise RuntimeError("Recusado: este script só escreve em abas TG_*. Aba: " + aba)
        ws = existentes.get(aba) or ss.add_worksheet(title=aba, rows=max(len(df) + 10, 50), cols=max(len(df.columns) + 2, 10))
        ws.clear()
        dados = valores_para_planilha(df)
        if len(dados) > ws.row_count:
            ws.add_rows(len(dados) - ws.row_count + 10)
        ws.update(range_name="A1", values=dados, value_input_option="RAW")
        print("Planilha:", aba, "<-", len(df), "linhas")
    return 0


if __name__ == "__main__":
    sys.exit(enviar())
