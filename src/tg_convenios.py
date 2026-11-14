"""
Pipeline de dados: Transferegov (dados abertos) -> controle de convênios e contratos de repasse de um município.

Fonte: arquivos CSV públicos do módulo "Transferências Discricionárias e Legais" do Transferegov.br,
publicados diariamente em https://api-publica.transferegov.gestao.gov.br/downloads/dadosgov/ (licença ODbL).
Não há API para este módulo: cada tabela é um zip com um CSV (separador ";", UTF-8 com BOM, datas dd/mm/aaaa,
decimal com vírgula). O script baixa só as tabelas que precisa, filtra o município e grava CSVs pequenos em data/.

Uso:
    python src/tg_convenios.py                 # usa config.json
    python src/tg_convenios.py --so-resumo     # não baixa nada; refaz o resumo a partir de data/

Decisões:
- Sem IA. É um pipeline de dados; o que decide algo é a pessoa que lê o resumo.
- Cada tabela baixada é filtrada em blocos (chunks) para caber na memória do GitHub Actions
  (proposta.zip tem ~200 MB; historico_situacao ~100 MB).
- A saída é determinística e commitável: o histórico do git vira o histórico dos convênios.
"""
import argparse
import csv
import datetime as dt
import io
import json
import os
import sys
import zipfile

import pandas as pd
import requests

BASE = "https://api-publica.transferegov.gestao.gov.br/downloads/dadosgov/"
RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PASTA_DADOS = os.path.join(RAIZ, "data")
CACHE = os.path.join(RAIZ, ".cache")
CSV_BLOCO = 200_000

# Tabelas usadas, na ordem de dependência. "chave" é a coluna usada para filtrar; "colunas" limita o que fica.
TABELAS = {
    "proposta": {
        "chave": "COD_MUNIC_IBGE",
        "colunas": ["ID_PROPOSTA", "NR_PROPOSTA", "UF_PROPONENTE", "MUNIC_PROPONENTE", "COD_MUNIC_IBGE",
                    "COD_ORGAO_SUP", "DESC_ORGAO_SUP", "COD_ORGAO", "DESC_ORGAO", "MODALIDADE",
                    "IDENTIF_PROPONENTE", "NM_PROPONENTE", "SIT_PROPOSTA", "OBJETO_PROPOSTA",
                    "VL_GLOBAL_PROP", "VL_REPASSE_PROP", "VL_CONTRAPARTIDA_PROP", "ANO_PROP"],
    },
    "convenio": {"chave": "ID_PROPOSTA", "colunas": None},
    "historico_situacao": {"chave": "NR_CONVENIO", "colunas": None},
    "termo_aditivo": {"chave": "NR_CONVENIO", "colunas": None},
    "prorroga_oficio": {"chave": "NR_CONVENIO", "colunas": None},
    "desbloqueio_cr": {"chave": "NR_CONVENIO", "colunas": None},
    "desembolso": {"chave": "NR_CONVENIO", "colunas": None},
    "empenho": {"chave": "NR_CONVENIO", "colunas": None},
    "cronograma_desembolso": {"chave": "NR_CONVENIO", "colunas": None},
    "acomp_obras_contratos_medicoes_modulo_empresas": {"chave": "ID_PROPOSTA", "colunas": None},
}

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------
def url_tabela(nome):
    return BASE + "siconv_" + nome + ".zip"


def baixar(nome, destino, sessao=None):
    """Baixa o zip de uma tabela para `destino` (streaming). Devolve o caminho."""
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    s = sessao or requests.Session()
    url = url_tabela(nome)
    with s.get(url, stream=True, timeout=600) as r:
        if r.status_code != 200:
            raise RuntimeError("Download falhou (" + str(r.status_code) + "): " + url)
        with open(destino, "wb") as f:
            for parte in r.iter_content(chunk_size=1 << 20):
                f.write(parte)
    return destino


def data_carga(sessao=None):
    """Data da extração publicada pelo Transferegov (data_carga_siconv.zip tem um CSV com a coluna DATA)."""
    try:
        s = sessao or requests.Session()
        r = s.get(BASE + "data_carga_siconv.zip", timeout=60)
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            with z.open(z.namelist()[0]) as f:
                linhas = f.read().decode("utf-8-sig").splitlines()
        return linhas[1].strip() if len(linhas) > 1 else ""
    except Exception as e:  # a data é informativa; não derruba o pipeline
        print("Aviso: não li data_carga_siconv.zip:", e)
        return ""


# ---------------------------------------------------------------------------
# Leitura e filtro
# ---------------------------------------------------------------------------
def filtrar_zip(caminho_zip, coluna, valores, colunas=None):
    """Lê o CSV dentro do zip em blocos e devolve só as linhas cuja `coluna` está em `valores`."""
    valores = set(str(v).strip() for v in valores)
    partes = []
    with zipfile.ZipFile(caminho_zip) as z:
        nome_csv = [n for n in z.namelist() if n.lower().endswith(".csv")][0]
        with z.open(nome_csv) as f:
            cab = next(csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig"), delimiter=";"))
        if coluna not in cab:
            raise RuntimeError("Coluna " + coluna + " não existe em " + os.path.basename(caminho_zip) +
                               ". Colunas: " + ", ".join(cab[:12]) + "... (o modelo de dados mudou?)")
        usar = [c for c in (colunas or cab) if c in cab]
        with z.open(nome_csv) as f:
            for bloco in pd.read_csv(f, sep=";", dtype=str, encoding="utf-8-sig", usecols=usar,
                                     chunksize=CSV_BLOCO, keep_default_na=False, on_bad_lines="warn"):
                sel = bloco[bloco[coluna].str.strip().isin(valores)]
                if len(sel):
                    partes.append(sel)
    if not partes:
        return pd.DataFrame(columns=usar)
    return pd.concat(partes, ignore_index=True)


def filtrar_proposta(caminho_zip, cfg):
    """Propostas do município: pelo código IBGE e, como rede de segurança, por UF + nome do município."""
    cod = str(cfg["cod_munic_ibge"]).strip()
    nome = cfg.get("municipio_nome", "").strip().upper()
    uf = cfg.get("uf", "").strip().upper()
    partes = []
    with zipfile.ZipFile(caminho_zip) as z:
        nome_csv = [n for n in z.namelist() if n.lower().endswith(".csv")][0]
        with z.open(nome_csv) as f:
            cab = next(csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig"), delimiter=";"))
        usar = [c for c in TABELAS["proposta"]["colunas"] if c in cab]
        with z.open(nome_csv) as f:
            for bloco in pd.read_csv(f, sep=";", dtype=str, encoding="utf-8-sig", usecols=usar,
                                     chunksize=CSV_BLOCO, keep_default_na=False, on_bad_lines="warn"):
                m = bloco["COD_MUNIC_IBGE"].str.strip().str.lstrip("0") == cod.lstrip("0")
                if nome and uf:
                    m = m | ((bloco["UF_PROPONENTE"].str.strip().str.upper() == uf) &
                             (bloco["MUNIC_PROPONENTE"].str.strip().str.upper() == nome))
                sel = bloco[m]
                if len(sel):
                    partes.append(sel)
    if not partes:
        return pd.DataFrame(columns=usar)
    df = pd.concat(partes, ignore_index=True)
    filtro_nome = [x.upper() for x in cfg.get("proponente_contem", []) if x]
    if filtro_nome:
        df = df[df["NM_PROPONENTE"].str.upper().apply(lambda n: any(f in n for f in filtro_nome))]
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Conversões (o CSV traz tudo como texto)
# ---------------------------------------------------------------------------
def numero(s):
    """'1.234,56' -> 1234.56; '1234.56' -> 1234.56; '' -> None."""
    if s is None:
        return None
    t = str(s).strip()
    if t == "" or t.upper() in ("NAN", "NONE"):
        return None
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None


def data(s):
    """'dd/mm/aaaa' (ou 'aaaa-mm-dd') -> datetime.date; vazio -> None."""
    if s is None:
        return None
    t = str(s).strip()[:10]
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(t, fmt).date()
        except ValueError:
            pass
    return None


def iso(d):
    return d.isoformat() if d else ""


def br(d):
    return d.strftime("%d/%m/%Y") if d else ""


def alerta_vigencia(dias, situacao=""):
    """Regra do controle: amarelo a 90 dias do fim, vermelho a 60; vencido se passou. Encerrados não alertam."""
    if dias is None:
        return ""
    s = (situacao or "").upper()
    if any(x in s for x in ("PRESTAÇÃO DE CONTAS", "PRESTACAO DE CONTAS", "CONCLU", "CANCELAD", "RESCINDID", "ENCERRAD", "ANULAD")):
        return "ENCERRADO"
    if dias < 0:
        return "VENCIDO"
    if dias <= 60:
        return "VERMELHO"
    if dias <= 90:
        return "AMARELO"
    return "OK"


# ---------------------------------------------------------------------------
# Resumo: uma linha por instrumento
# ---------------------------------------------------------------------------
def montar_resumo(t, hoje):
    """t: dict nome_tabela -> DataFrame (texto). Devolve lista de dicts, um por convênio."""
    prop = t["proposta"].set_index("ID_PROPOSTA") if len(t["proposta"]) else pd.DataFrame()
    conv = t["convenio"]
    linhas = []
    for _, c in conv.iterrows():
        nr = str(c["NR_CONVENIO"]).strip()
        p = prop.loc[c["ID_PROPOSTA"]] if len(prop) and c["ID_PROPOSTA"] in prop.index else {}
        if isinstance(p, pd.DataFrame):      # ID repetido na proposta: fica com a primeira
            p = p.iloc[0]
        fim = data(c.get("DIA_FIM_VIGENC_CONV"))
        dias = (fim - hoje).days if fim else None
        vl_rep = numero(c.get("VL_REPASSE_CONV"))
        vl_des = numero(c.get("VL_DESEMBOLSADO_CONV"))
        pct = round(100 * vl_des / vl_rep, 1) if vl_rep and vl_des is not None else None

        hist = t["historico_situacao"]
        h = hist[hist["NR_CONVENIO"].str.strip() == nr] if len(hist) else hist
        ult_sit, ult_sit_dt = "", None
        if len(h):
            h2 = h.assign(_d=h["DIA_HISTORICO_SIT"].map(data)).sort_values("_d")
            ult_sit, ult_sit_dt = h2.iloc[-1]["HISTORICO_SIT"], h2.iloc[-1]["_d"]

        des = t["desembolso"]
        d = des[des["NR_CONVENIO"].str.strip() == nr] if len(des) else des
        ult_des_dt, ult_des_vl = None, None
        if len(d):
            d2 = d.assign(_d=d["DATA_DESEMBOLSO"].map(data)).sort_values("_d")
            ult_des_dt, ult_des_vl = d2.iloc[-1]["_d"], numero(d2.iloc[-1]["VL_DESEMBOLSADO"])

        med = t["acomp_obras_contratos_medicoes_modulo_empresas"]
        m = med[med["ID_PROPOSTA"].str.strip() == str(c["ID_PROPOSTA"]).strip()] if len(med) else med
        ult_med_n, ult_med_sit, ult_med_fim = "", "", None
        if len(m):
            m2 = m.assign(_n=pd.to_numeric(m["NUMERO_MEDICAO_ACOMPANHAMENTO_OBRA"], errors="coerce")).sort_values("_n")
            ult_med_n = str(m2.iloc[-1]["NUMERO_MEDICAO_ACOMPANHAMENTO_OBRA"])
            ult_med_sit = m2.iloc[-1]["SITUACAO_MEDICAO_ACOMPANHAMENTO_OBRA"]
            ult_med_fim = data(m2.iloc[-1]["DATA_FIM_MEDICAO_OBJETO_ACOMPANHAMENTO_OBRA"])

        desb = t["desbloqueio_cr"]
        b = desb[desb["NR_CONVENIO"].str.strip() == nr] if len(desb) else desb
        bloqueado = sum((numero(x) or 0) for x in b["VL_BLOQUEADO"]) if len(b) else 0.0

        ta = t["termo_aditivo"]
        qtd_ta = int((ta["NR_CONVENIO"].str.strip() == nr).sum()) if len(ta) else 0
        pro = t["prorroga_oficio"]
        qtd_pro = int((pro["NR_CONVENIO"].str.strip() == nr).sum()) if len(pro) else 0

        linhas.append({
            "NR_CONVENIO": nr,
            "ORGAO_SUPERIOR": p.get("DESC_ORGAO_SUP", "") if len(prop) else "",
            "ORGAO": p.get("DESC_ORGAO", "") if len(prop) else "",
            "MODALIDADE": p.get("MODALIDADE", "") if len(prop) else "",
            "PROPONENTE": p.get("NM_PROPONENTE", "") if len(prop) else "",
            "OBJETO": (p.get("OBJETO_PROPOSTA", "") if len(prop) else "").strip(),
            "SITUACAO": c.get("SIT_CONVENIO", ""),
            "SUBSITUACAO": c.get("SUBSITUACAO_CONV", ""),
            "INSTRUMENTO_ATIVO": c.get("INSTRUMENTO_ATIVO", ""),
            "ASSINATURA": iso(data(c.get("DIA_ASSIN_CONV"))),
            "INICIO_VIGENCIA": iso(data(c.get("DIA_INIC_VIGENC_CONV"))),
            "FIM_VIGENCIA": iso(fim),
            "FIM_VIGENCIA_ORIGINAL": iso(data(c.get("DIA_FIM_VIGENC_ORIGINAL_CONV"))),
            "DIAS_PARA_FIM": dias,
            "ALERTA_VIGENCIA": alerta_vigencia(dias, c.get("SIT_CONVENIO", "")),
            "LIMITE_PREST_CONTAS": iso(data(c.get("DIA_LIMITE_PREST_CONTAS"))),
            "VL_GLOBAL": numero(c.get("VL_GLOBAL_CONV")),
            "VL_REPASSE": vl_rep,
            "VL_CONTRAPARTIDA": numero(c.get("VL_CONTRAPARTIDA_CONV")),
            "VL_EMPENHADO": numero(c.get("VL_EMPENHADO_CONV")),
            "VL_DESEMBOLSADO": vl_des,
            "PCT_DESEMBOLSADO": pct,
            "VL_SALDO_CONTA": numero(c.get("VL_SALDO_CONTA")),
            "VL_BLOQUEADO_CR": round(bloqueado, 2),
            "QTD_TA": qtd_ta if qtd_ta else (int(numero(c.get("QTD_TA")) or 0)),
            "QTD_PRORROGA": qtd_pro if qtd_pro else (int(numero(c.get("QTD_PRORROGA")) or 0)),
            "ULTIMA_SITUACAO": ult_sit,
            "ULTIMA_SITUACAO_EM": iso(ult_sit_dt),
            "ULTIMO_DESEMBOLSO_EM": iso(ult_des_dt),
            "ULTIMO_DESEMBOLSO_VL": ult_des_vl,
            "ULTIMA_MEDICAO": ult_med_n,
            "ULTIMA_MEDICAO_SITUACAO": ult_med_sit,
            "ULTIMA_MEDICAO_FIM": iso(ult_med_fim),
        })
    ordem = {"VENCIDO": 0, "VERMELHO": 1, "AMARELO": 2, "OK": 3, "": 4, "ENCERRADO": 5}
    linhas.sort(key=lambda l: (ordem.get(l["ALERTA_VIGENCIA"], 9), l["DIAS_PARA_FIM"] if l["DIAS_PARA_FIM"] is not None else 10**6))
    return linhas


def dinheiro(v):
    if v is None:
        return ""
    return "R$ " + ("{:,.2f}".format(v)).replace(",", "X").replace(".", ",").replace("X", ".")


def resumo_markdown(linhas, meta):
    hoje = meta["gerado_em"][:10]
    t = ["# Convênios e contratos de repasse — " + meta.get("municipio_nome", "") + "/" + meta.get("uf", ""), ""]
    t.append("Fonte: dados abertos do Transferegov.br (carga de **" + (meta.get("data_carga") or "?") +
             "**), gerado em " + hoje + ". " + str(len(linhas)) + " instrumento(s). Sem IA: só leitura, junção e regras.")
    t.append("")
    ativos = [l for l in linhas if l["ALERTA_VIGENCIA"] not in ("ENCERRADO", "")]
    alertas = [l for l in ativos if l["ALERTA_VIGENCIA"] in ("VENCIDO", "VERMELHO", "AMARELO")]
    t.append("## Alertas de vigência (" + str(len(alertas)) + ")")
    t.append("")
    if alertas:
        for l in alertas:
            t.append("- **" + l["ALERTA_VIGENCIA"] + "** · " + l["NR_CONVENIO"] + " · " + (l["ORGAO_SUPERIOR"] or "") +
                     " · fim " + br(data(l["FIM_VIGENCIA"])) + " (" + str(l["DIAS_PARA_FIM"]) + " dias) · " + l["SITUACAO"])
    else:
        t.append("Nenhum instrumento ativo vence nos próximos 90 dias.")
    t.append("")
    t.append("## Todos os instrumentos")
    t.append("")
    t.append("| Nº | Órgão | Situação | Fim vigência | Dias | Alerta | Repasse | Desemb. % | Saldo conta | TA | Prorr. | Último BM | Última situação |")
    t.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for l in linhas:
        t.append("| " + " | ".join([
            l["NR_CONVENIO"], (l["ORGAO_SUPERIOR"] or "")[:28], (l["SITUACAO"] or ""), br(data(l["FIM_VIGENCIA"])),
            "" if l["DIAS_PARA_FIM"] is None else str(l["DIAS_PARA_FIM"]), l["ALERTA_VIGENCIA"],
            dinheiro(l["VL_REPASSE"]), "" if l["PCT_DESEMBOLSADO"] is None else str(l["PCT_DESEMBOLSADO"]).replace(".", ",") + "%",
            dinheiro(l["VL_SALDO_CONTA"]), str(l["QTD_TA"]), str(l["QTD_PRORROGA"]),
            (l["ULTIMA_MEDICAO"] + " " + (l["ULTIMA_MEDICAO_SITUACAO"] or "")).strip(),
            ((l["ULTIMA_SITUACAO"] or "")[:40] + (" (" + br(data(l["ULTIMA_SITUACAO_EM"])) + ")" if l["ULTIMA_SITUACAO_EM"] else "")),
        ]).replace("\n", " ") + " |")
    t.append("")
    t.append("Regra de alerta: AMARELO a 90 dias do fim da vigência, VERMELHO a 60, VENCIDO depois; ENCERRADO quando a situação é de prestação de contas/concluído/cancelado.")
    return "\n".join(t) + "\n"


# ---------------------------------------------------------------------------
# Orquestração
# ---------------------------------------------------------------------------
def carregar_config(caminho=None):
    with open(caminho or os.path.join(RAIZ, "config.json"), encoding="utf-8") as f:
        return json.load(f)


def executar(cfg, hoje=None, baixar_fn=baixar, pasta_saida=None, cache=None):
    hoje = hoje or dt.date.today()
    pasta_saida = pasta_saida or PASTA_DADOS
    cache = cache or CACHE
    os.makedirs(pasta_saida, exist_ok=True)
    sessao = requests.Session()
    t = {}

    print("proposta: baixando e filtrando o município", cfg["cod_munic_ibge"])
    t["proposta"] = filtrar_proposta(baixar_fn("proposta", os.path.join(cache, "siconv_proposta.zip"), sessao), cfg)
    ids_proposta = set(t["proposta"]["ID_PROPOSTA"].str.strip())
    if not ids_proposta:
        raise RuntimeError("Nenhuma proposta encontrada para o município. Confira cod_munic_ibge / municipio_nome em config.json.")
    print("  propostas do município:", len(ids_proposta))

    t["convenio"] = filtrar_zip(baixar_fn("convenio", os.path.join(cache, "siconv_convenio.zip"), sessao), "ID_PROPOSTA", ids_proposta)
    extras = set(str(x).strip() for x in cfg.get("instrumentos_extra", []) if str(x).strip())
    nrs = set(t["convenio"]["NR_CONVENIO"].str.strip()) | extras
    if extras - set(t["convenio"]["NR_CONVENIO"].str.strip()):
        faltam = extras - set(t["convenio"]["NR_CONVENIO"].str.strip())
        print("  instrumentos_extra não achados pelo município; buscando por número:", ", ".join(sorted(faltam)))
        mais = filtrar_zip(os.path.join(cache, "siconv_convenio.zip"), "NR_CONVENIO", faltam)
        t["convenio"] = pd.concat([t["convenio"], mais], ignore_index=True).drop_duplicates("NR_CONVENIO")
        ids_proposta |= set(mais["ID_PROPOSTA"].str.strip())
    print("  convênios/contratos de repasse:", len(t["convenio"]))

    for nome, spec in TABELAS.items():
        if nome in ("proposta", "convenio"):
            continue
        chave = spec["chave"]
        valores = ids_proposta if chave == "ID_PROPOSTA" else nrs
        print(nome + ": baixando e filtrando")
        t[nome] = filtrar_zip(baixar_fn(nome, os.path.join(cache, "siconv_" + nome + ".zip"), sessao), chave, valores, spec["colunas"])
        print("  linhas:", len(t[nome]))

    meta = {"gerado_em": dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds"),
            "data_carga": data_carga(sessao), "cod_munic_ibge": str(cfg["cod_munic_ibge"]),
            "municipio_nome": cfg.get("municipio_nome", ""), "uf": cfg.get("uf", ""),
            "fonte": BASE, "licenca": "ODbL 1.0",
            "tabelas": {k: int(len(v)) for k, v in t.items()}}
    gravar_saida(t, meta, hoje, pasta_saida)
    return t, meta


def gravar_saida(t, meta, hoje, pasta_saida):
    for nome, df in t.items():
        df.to_csv(os.path.join(pasta_saida, nome + ".csv"), index=False, sep=";", encoding="utf-8-sig")
    linhas = montar_resumo(t, hoje)
    with open(os.path.join(pasta_saida, "resumo.json"), "w", encoding="utf-8") as f:
        json.dump({"meta": meta, "instrumentos": linhas}, f, ensure_ascii=False, indent=1)
    pd.DataFrame(linhas).to_csv(os.path.join(pasta_saida, "resumo.csv"), index=False, sep=";", encoding="utf-8-sig")
    with open(os.path.join(pasta_saida, "resumo.md"), "w", encoding="utf-8") as f:
        f.write(resumo_markdown(linhas, meta))
    with open(os.path.join(pasta_saida, "_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=1)
    print("Resumo:", len(linhas), "instrumento(s);",
          sum(1 for l in linhas if l["ALERTA_VIGENCIA"] in ("VENCIDO", "VERMELHO", "AMARELO")), "com alerta de vigência.")
    return linhas


def so_resumo(cfg, hoje=None, pasta=None):
    """Refaz resumo.* a partir dos CSVs já em data/ (sem baixar nada)."""
    pasta = pasta or PASTA_DADOS
    t = {}
    for nome in TABELAS:
        caminho = os.path.join(pasta, nome + ".csv")
        t[nome] = pd.read_csv(caminho, sep=";", dtype=str, encoding="utf-8-sig", keep_default_na=False) if os.path.exists(caminho) else pd.DataFrame()
    with open(os.path.join(pasta, "_meta.json"), encoding="utf-8") as f:
        meta = json.load(f)
    return gravar_saida(t, meta, hoje or dt.date.today(), pasta)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--config", default=None)
    ap.add_argument("--so-resumo", action="store_true", help="não baixa; refaz o resumo a partir de data/")
    args = ap.parse_args(argv)
    cfg = carregar_config(args.config)
    if args.so_resumo:
        so_resumo(cfg)
    else:
        executar(cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
