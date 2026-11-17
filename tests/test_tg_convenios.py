"""
Testes do pipeline sem internet: os zips do Transferegov são simulados com o mesmo formato
(CSV ';', UTF-8 com BOM, datas dd/mm/aaaa, decimal com vírgula) e as colunas do modelo oficial.

    python -m pytest -q
"""
import datetime as dt
import io
import json
import os
import sys
import zipfile

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import tg_convenios as tg  # noqa: E402

HOJE = dt.date(2026, 11, 14)

# Município fictício 9999999 e dois instrumentos fictícios; um terceiro de outro município para o filtro descartar.
PROPOSTA = [
    ["ID_PROPOSTA", "NR_PROPOSTA", "UF_PROPONENTE", "MUNIC_PROPONENTE", "COD_MUNIC_IBGE", "COD_ORGAO_SUP", "DESC_ORGAO_SUP",
     "COD_ORGAO", "DESC_ORGAO", "MODALIDADE", "IDENTIF_PROPONENTE", "NM_PROPONENTE", "SIT_PROPOSTA", "OBJETO_PROPOSTA",
     "VL_GLOBAL_PROP", "VL_REPASSE_PROP", "VL_CONTRAPARTIDA_PROP", "ANO_PROP", "CEP_PROPONENTE"],
    ["1001", "1/2018", "XX", "CIDADE EXEMPLO", "9999999", "56000", "MINISTERIO EXEMPLO A", "56001", "CAIXA", "CONTRATO DE REPASSE",
     "00.000.000/0001-00", "MUNICIPIO DE CIDADE EXEMPLO", "Proposta/Plano de Trabalho Aprovados", "Pavimentação da Rua A", "1.000.000,00", "900.000,00", "100.000,00", "2018", "00000-000"],
    ["1002", "2/2021", "XX", "CIDADE EXEMPLO", "9999999", "26000", "MINISTERIO EXEMPLO B", "26001", "CAIXA", "CONTRATO DE REPASSE",
     "00.000.000/0001-00", "MUNICIPIO DE CIDADE EXEMPLO", "Proposta/Plano de Trabalho Aprovados", "Construção de quadra", "500.000,00", "450.000,00", "50.000,00", "2021", "00000-000"],
    ["1003", "4/2019", "XX", "CIDADE EXEMPLO", "9999999", "56000", "MINISTERIO EXEMPLO A", "56001", "CAIXA", "CONTRATO DE REPASSE",
     "00.000.000/0001-00", "MUNICIPIO DE CIDADE EXEMPLO", "Proposta/Plano de Trabalho Aprovados", "Drenagem", "300.000,00", "300.000,00", "0,00", "2019", "00000-000"],
    ["1004", "5/2012", "XX", "CIDADE EXEMPLO", "9999999", "56000", "MINISTERIO EXEMPLO A", "56001", "CAIXA", "CONVENIO",
     "00.000.000/0001-00", "MUNICIPIO DE CIDADE EXEMPLO", "Proposta/Plano de Trabalho Aprovados", "Nunca assinado", "50.000,00", "50.000,00", "0,00", "2012", "00000-000"],
    ["2001", "3/2020", "YY", "OUTRA CIDADE", "8888888", "26000", "MINISTERIO EXEMPLO B", "26001", "CAIXA", "CONVENIO",
     "11.111.111/0001-11", "MUNICIPIO DE OUTRA CIDADE", "Proposta/Plano de Trabalho Aprovados", "Outra coisa", "1,00", "1,00", "0,00", "2020", "00000-000"],
]
CONVENIO_CAB = ["NR_CONVENIO", "ID_PROPOSTA", "DIA", "MES", "ANO", "DIA_ASSIN_CONV", "SIT_CONVENIO", "SUBSITUACAO_CONV", "SITUACAO_PUBLICACAO",
                "INSTRUMENTO_ATIVO", "IND_OPERA_OBTV", "NR_PROCESSO", "UG_EMITENTE", "DIA_PUBL_CONV", "DIA_INIC_VIGENC_CONV", "DIA_FIM_VIGENC_CONV",
                "DIA_FIM_VIGENC_ORIGINAL_CONV", "DIAS_PREST_CONTAS", "DIA_LIMITE_PREST_CONTAS", "DATA_SUSPENSIVA", "DATA_RETIRADA_SUSPENSIVA",
                "DIAS_CLAUSULA_SUSPENSIVA", "SITUACAO_CONTRATACAO", "IND_ASSINADO", "MOTIVO_SUSPENSAO", "IND_FOTO", "QTDE_CONVENIOS", "QTD_TA",
                "QTD_PRORROGA", "VL_GLOBAL_CONV", "VL_REPASSE_CONV", "VL_CONTRAPARTIDA_CONV", "VL_EMPENHADO_CONV", "VL_DESEMBOLSADO_CONV",
                "VL_SALDO_REMAN_TESOURO", "VL_SALDO_REMAN_CONVENENTE", "VL_RENDIMENTO_APLICACAO", "VL_INGRESSO_CONTRAPARTIDA", "VL_SALDO_CONTA",
                "VALOR_GLOBAL_ORIGINAL_CONV"]


def linha_convenio(nr, idp, fim, sit="Em execução", repasse="900.000,00", desemb="450.000,00", ta="2", pro="1", saldo="12.345,67", assinado="SIM", limite_pc=""):
    v = {c: "" for c in CONVENIO_CAB}
    v.update({"NR_CONVENIO": nr, "ID_PROPOSTA": idp, "DIA_ASSIN_CONV": "10/12/2018", "SIT_CONVENIO": sit, "INSTRUMENTO_ATIVO": "SIM",
              "IND_ASSINADO": assinado, "DIA_LIMITE_PREST_CONTAS": limite_pc,
              "DIA_INIC_VIGENC_CONV": "10/12/2018", "DIA_FIM_VIGENC_CONV": fim, "DIA_FIM_VIGENC_ORIGINAL_CONV": "10/12/2020",
              "QTD_TA": ta, "QTD_PRORROGA": pro, "VL_GLOBAL_CONV": "1.000.000,00",
              "VL_REPASSE_CONV": repasse, "VL_CONTRAPARTIDA_CONV": "100.000,00", "VL_EMPENHADO_CONV": repasse,
              "VL_DESEMBOLSADO_CONV": desemb, "VL_SALDO_CONTA": saldo})
    return [v[c] for c in CONVENIO_CAB]


TABELAS = {
    "proposta": PROPOSTA,
    "convenio": [CONVENIO_CAB,
                 linha_convenio("800001/2018", "1001", "31/12/2026"),                      # vence em 47 dias -> VERMELHO
                 linha_convenio("800002/2021", "1002", "30/06/2027", desemb="0,00", ta="0", pro="0", saldo="0,00"),  # OK
                 linha_convenio("800003/2019", "1003", "26/10/2026", sit="Aguardando Prestação de Contas", limite_pc="24/12/2026"),  # PC pendente
                 linha_convenio("800004/2012", "1004", "30/12/2012", sit="Proposta/Plano de Trabalho Aprovado", assinado="NÃO"),   # nunca assinado
                 linha_convenio("700001/2020", "2001", "01/01/2027")],                     # outro município: fora
    "historico_situacao": [["ID_PROPOSTA", "NR_CONVENIO", "DIA_HISTORICO_SIT", "HISTORICO_SIT", "DIAS_HISTORICO_SIT", "COD_HISTORICO_SIT"],
                           ["1001", "800001/2018", "10/12/2018", "Assinado", "0", "1"],
                           ["1001", "800001/2018", "15/03/2019", "Em execução", "95", "2"],
                           ["2001", "700001/2020", "01/01/2021", "Assinado", "0", "1"]],
    "termo_aditivo": [["NR_CONVENIO", "ID_SOLICITACAO", "NUMERO_TA", "TIPO_TA", "VL_GLOBAL_TA", "VL_REPASSE_TA", "VL_CONTRAPARTIDA_TA", "DT_ASSINATURA_TA", "DT_INICIO_TA", "DT_FIM_TA", "JUSTIFICATIVA_TA"],
                      ["800001/2018", "1", "1", "Prazo", "", "", "", "01/12/2020", "01/12/2020", "31/12/2024", "x"],
                      ["800001/2018", "2", "2", "Prazo", "", "", "", "01/12/2024", "01/12/2024", "31/12/2026", "y"]],
    "prorroga_oficio": [["NR_CONVENIO", "NR_PRORROGA", "DT_INICIO_PRORROGA", "DT_FIM_PRORROGA", "DIAS_PRORROGA", "DT_ASSINATURA_PRORROGA", "SIT_PRORROGA"],
                        ["800001/2018", "1", "01/01/2024", "01/03/2024", "60", "01/01/2024", "Vigente"]],
    "desbloqueio_cr": [["NR_CONVENIO", "NR_OB", "DATA_CADASTRO", "DATA_ENVIO", "TIPO_RECURSO_DESBLOQUEIO", "VL_TOTAL_DESBLOQUEIO", "VL_DESBLOQUEADO", "VL_BLOQUEADO"],
                       ["800001/2018", "1", "01/10/2026", "02/10/2026", "REPASSE", "100.000,00", "60.000,00", "40.000,00"],
                       ["800001/2018", "2", "01/11/2026", "", "REPASSE", "10.000,00", "0,00", "10.000,00"]],
    "desembolso": [["ID_DESEMBOLSO", "NR_CONVENIO", "DT_ULT_DESEMBOLSO", "QTD_DIAS_SEM_DESEMBOLSO", "DATA_DESEMBOLSO", "ANO_DESEMBOLSO", "MES_DESEMBOLSO", "NR_SIAFI", "UG_EMITENTE_DH", "OBSERVACAO_DH", "VL_DESEMBOLSADO"],
                   ["1", "800001/2018", "", "", "05/05/2025", "2025", "5", "x", "x", "", "200.000,00"],
                   ["2", "800001/2018", "", "", "20/09/2026", "2026", "9", "x", "x", "", "250.000,00"]],
    "empenho": [["ID_EMPENHO", "NR_CONVENIO", "NR_EMPENHO", "TIPO_NOTA", "DESC_TIPO_NOTA", "DATA_EMISSAO", "COD_SITUACAO_EMPENHO", "DESC_SITUACAO_EMPENHO", "UG_EMITENTE", "UG_RESPONSAVEL", "FONTE_RECURSO", "NATUREZA_DESPESA", "PLANO_INTERNO", "PTRES", "VALOR_EMPENHO", "RESULTADO_PRIMARIO", "OBSERVACAO_EMPENHO", "DESCRICAO_EMENDA_SIAFI"],
                ["1", "800001/2018", "2018NE1", "", "", "10/12/2018", "", "", "", "", "", "", "", "", "900.000,00", "", "", ""]],
    "cronograma_desembolso": [["NR_CONVENIO", "ID_PROPOSTA", "NR_PARCELA_CRONO_DESEMBOLSO", "MES_CRONO_DESEMBOLSO", "ANO_CRONO_DESEMBOLSO", "TIPO_RESP_CRONO_DESEMBOLSO", "VALOR_PARCELA_CRONO_DESEMBOLSO"],
                              ["800001/2018", "1001", "1", "12", "2018", "Concedente", "900.000,00"]],
    "acomp_obras_contratos_medicoes_modulo_empresas": [["ID_PROPOSTA", "ID_CONTRATO_MEDICAO_ACOMPANHAMENTO_OBRA", "ID_MEDICAO_ACOMPANHAMENTO_OBRA", "DATA_INICIO_OBRA_CONTRATO_ACOMPANHAMENTO_OBRA", "CNPJ_FORNECEDOR_CONTRATO_ACOMPANHAMENTO_OBRA", "NUMERO_MEDICAO_ACOMPANHAMENTO_OBRA", "SITUACAO_MEDICAO_ACOMPANHAMENTO_OBRA", "DATA_INICIO_MEDICAO_OBJETO_ACOMPANHAMENTO_OBRA", "DATA_FIM_MEDICAO_OBJETO_ACOMPANHAMENTO_OBRA", "QTD_DIAS_SEM_MEDICAO_ACOMPANHAMENTO_OBRA"],
                                                       ["1001", "c1", "m1", "01/02/2025", "00.000.000/0001-99", "1", "Aceita", "01/02/2025", "28/02/2025", "0"],
                                                       ["1001", "c1", "m2", "01/02/2025", "00.000.000/0001-99", "10", "Em análise", "01/08/2026", "31/08/2026", "0"],
                                                       ["1001", "c1", "m3", "01/02/2025", "00.000.000/0001-99", "2", "Aceita", "01/03/2025", "31/03/2025", "0"]],
}


def zip_de(linhas, nome):
    buf = io.StringIO()
    for l in linhas:
        buf.write(";".join(l) + "\n")
    z = io.BytesIO()
    with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(nome + ".csv", ("﻿" + buf.getvalue()).encode("utf-8"))
    return z.getvalue()


@pytest.fixture
def ambiente(tmp_path):
    fonte = tmp_path / "fonte"
    fonte.mkdir()
    for nome, linhas in TABELAS.items():
        (fonte / ("siconv_" + nome + ".zip")).write_bytes(zip_de(linhas, "siconv_" + nome))

    def baixar_falso(nome, destino, sessao=None):
        os.makedirs(os.path.dirname(destino), exist_ok=True)
        with open(fonte / ("siconv_" + nome + ".zip"), "rb") as f, open(destino, "wb") as g:
            g.write(f.read())
        return destino

    cfg = {"cod_munic_ibge": "9999999", "municipio_nome": "CIDADE EXEMPLO", "uf": "XX", "proponente_contem": [], "instrumentos_extra": []}
    return {"cfg": cfg, "baixar": baixar_falso, "saida": str(tmp_path / "data"), "cache": str(tmp_path / "cache")}


def rodar(amb, **extra):
    cfg = dict(amb["cfg"], **extra)
    tg.data_carga = lambda sessao=None: "14/11/2026"   # sem internet
    tg.catalogo_tabelas = lambda sessao=None: []
    return tg.executar(cfg, hoje=HOJE, baixar_fn=amb["baixar"], pasta_saida=amb["saida"], cache=amb["cache"])


def test_numero_e_data():
    assert tg.numero("1.234,56") == 1234.56
    assert tg.numero("1234.56") == 1234.56
    assert tg.numero("0,00") == 0.0
    assert tg.numero("") is None
    assert tg.data("31/12/2026") == dt.date(2026, 12, 31)
    assert tg.data("2026-12-31") == dt.date(2026, 12, 31)
    assert tg.data("") is None


def test_alerta_vigencia():
    assert tg.alerta_vigencia(200) == "OK"
    assert tg.alerta_vigencia(90) == "AMARELO"
    assert tg.alerta_vigencia(61) == "AMARELO"
    assert tg.alerta_vigencia(60) == "VERMELHO"
    assert tg.alerta_vigencia(0) == "VERMELHO"
    assert tg.alerta_vigencia(-1) == "VENCIDO"
    assert tg.alerta_vigencia(-500, "Prestação de Contas Aprovada") == "ENCERRADO"
    assert tg.alerta_vigencia(-500, "Prestação de Contas em Análise") == "ENCERRADO"
    assert tg.alerta_vigencia(-100, "Convênio Anulado") == "ENCERRADO"
    assert tg.alerta_vigencia(-19, "Aguardando Prestação de Contas") == "PRESTAR CONTAS"     # não é encerrado: tem prazo correndo
    assert tg.alerta_vigencia(-5433, "Proposta/Plano de Trabalho Aprovado", "NÃO") == "NÃO ASSINADO"
    assert tg.alerta_vigencia(-5433, "", "NÃO") == "NÃO ASSINADO"
    assert tg.alerta_vigencia(None) == ""


def test_filtra_so_o_municipio_e_junta_as_tabelas(ambiente):
    t, meta = rodar(ambiente)
    assert sorted(t["proposta"]["ID_PROPOSTA"]) == ["1001", "1002", "1003", "1004"]
    assert sorted(t["convenio"]["NR_CONVENIO"]) == ["800001/2018", "800002/2021", "800003/2019", "800004/2012"]
    assert list(t["historico_situacao"]["NR_CONVENIO"].unique()) == ["800001/2018"]   # o de outro município ficou de fora
    assert meta["tabelas"]["convenio"] == 4
    assert "CEP_PROPONENTE" not in t["proposta"].columns                               # só as colunas escolhidas


def test_resumo_calcula_dias_alerta_percentual_e_ultimos(ambiente):
    t, meta = rodar(ambiente)
    with open(os.path.join(ambiente["saida"], "resumo.json"), encoding="utf-8") as f:
        r = json.load(f)
    por = {l["NR_CONVENIO"]: l for l in r["instrumentos"]}
    a = por["800001/2018"]
    assert a["DIAS_PARA_FIM"] == 47 and a["ALERTA_VIGENCIA"] == "VERMELHO"
    assert a["PCT_DESEMBOLSADO"] == 50.0
    assert a["QTD_TA"] == 2 and a["QTD_PRORROGA"] == 1
    assert a["VL_BLOQUEADO_CR"] == 50000.0
    assert a["ULTIMA_SITUACAO"] == "Em execução" and a["ULTIMA_SITUACAO_EM"] == "2019-03-15"
    assert a["ULTIMO_DESEMBOLSO_EM"] == "2026-09-20" and a["ULTIMO_DESEMBOLSO_VL"] == 250000.0
    assert a["ULTIMA_MEDICAO"] == "10" and a["ULTIMA_MEDICAO_SITUACAO"] == "Em análise"   # 10 vem depois de 2 (ordem numérica, não texto)
    assert a["ORGAO_SUPERIOR"] == "MINISTERIO EXEMPLO A" and a["OBJETO"] == "Pavimentação da Rua A"
    b = por["800002/2021"]
    assert b["ALERTA_VIGENCIA"] == "OK" and b["PCT_DESEMBOLSADO"] == 0.0 and b["ULTIMA_MEDICAO"] == ""
    c = por["800003/2019"]
    assert c["ALERTA_VIGENCIA"] == "PRESTAR CONTAS" and c["DIAS_PARA_PREST_CONTAS"] == 40
    d = por["800004/2012"]
    assert d["ALERTA_VIGENCIA"] == "NÃO ASSINADO" and d["ASSINADO"] == "NÃO"
    assert [l["NR_CONVENIO"] for l in r["instrumentos"]] == ["800003/2019", "800001/2018", "800002/2021", "800004/2012"]   # PC pendente > vermelho > ok > não assinado


def test_saidas_gravadas_e_markdown(ambiente):
    rodar(ambiente)
    arquivos = set(os.listdir(ambiente["saida"]))
    for nome in list(TABELAS) + ["resumo"]:
        assert nome + ".csv" in arquivos
    assert {"resumo.json", "resumo.md", "_meta.json"} <= arquivos
    md = open(os.path.join(ambiente["saida"], "resumo.md"), encoding="utf-8").read()
    assert "## Alertas (2)" in md
    assert "**VERMELHO** · 800001/2018" in md
    assert "**PRESTAR CONTAS** · 800003/2019" in md and "limite da PC 24/12/2026 (40 dias)" in md
    assert "nunca assinadas (fora dos alertas): 800004/2012" in md
    assert "R$ 900.000,00" in md
    assert "carga de **14/11/2026**" in md
    assert "Sem IA" in md


def test_so_resumo_refaz_a_partir_dos_csvs(ambiente):
    rodar(ambiente)
    os.remove(os.path.join(ambiente["saida"], "resumo.md"))
    linhas = tg.so_resumo(ambiente["cfg"], hoje=HOJE, pasta=ambiente["saida"])
    assert len(linhas) == 4 and os.path.exists(os.path.join(ambiente["saida"], "resumo.md"))


def test_instrumento_extra_entra_pelo_numero(ambiente):
    t, _ = rodar(ambiente, instrumentos_extra=["700001/2020"])
    assert "700001/2020" in set(t["convenio"]["NR_CONVENIO"])
    assert "2001" in set(t["historico_situacao"]["ID_PROPOSTA"])


def test_municipio_sem_proposta_falha_com_mensagem_clara(ambiente):
    with pytest.raises(RuntimeError, match="Nenhuma proposta"):
        rodar(ambiente, cod_munic_ibge="1234567", municipio_nome="NADA", uf="ZZ")


def test_coluna_ausente_falha_explicando(tmp_path):
    z = tmp_path / "x.zip"
    z.write_bytes(zip_de([["A", "B"], ["1", "2"]], "x"))
    with pytest.raises(RuntimeError, match="modelo de dados mudou"):
        tg.filtrar_zip(str(z), "NR_CONVENIO", {"1"})


def test_planilha_so_escreve_em_abas_tg():
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
    import planilha
    assert planilha.nome_aba("convenio") == "TG_CONVENIO"
    assert planilha.nome_aba("acomp_obras_contratos_medicoes_modulo_empresas") == "TG_MEDICOES"
    assert all(planilha.nome_aba(a).startswith("TG_") for a in planilha.ABAS)
    v = planilha.valores_para_planilha(pd.DataFrame({"a": ["1", None], "b": ["x", "y"]}))
    assert v == [["a", "b"], ["1", "x"], ["", "y"]]


def test_tabela_opcional_ausente_nao_derruba(ambiente):
    t, _ = rodar(ambiente)          # fixture não tem "contrato": só avisa
    assert "contrato" not in t


def test_tabela_opcional_contrato_e_filtrada_pelo_municipio(ambiente, tmp_path):
    fonte = tmp_path / "fonte"
    (fonte / "siconv_contrato.zip").write_bytes(zip_de([
        ["ID_PROPOSTA", "NR_CONTRATO", "CNPJ"],
        ["1001", "463/2021", "11"],
        ["zzz", "999/2020", "22"],          # outro município: não entra
    ], "siconv_contrato"))
    t, _ = rodar(ambiente)
    assert list(t["contrato"]["NR_CONTRATO"]) == ["463/2021"]
    assert os.path.exists(os.path.join(ambiente["saida"], "contrato.csv"))
