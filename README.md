# Convênios e contratos de repasse — pipeline de dados do Transferegov

Automação em **Python + GitHub Actions** que, todo dia útil, baixa os dados abertos do Transferegov.br, filtra os convênios e contratos de repasse de **um município** (Igarassu/PE, configurável) e gera um controle pronto para leitura: um resumo com um instrumento por linha, alertas de fim de vigência, percentual desembolsado, saldo em conta, termos aditivos, prorrogas, último desembolso, última situação e último boletim de medição.

A saída é commitada no próprio repositório: **o histórico do git é o histórico dos convênios** (`git log -- data/resumo.csv` mostra quando cada situação mudou). Opcionalmente, as mesmas tabelas vão para abas de uma planilha Google.

**Sem IA.** É um pipeline de dados: download, filtro, junção e regras. Quem decide algo é a pessoa que lê o resumo.

## Status

**Rodando.** Primeira execução real em 14/11/2026 (carga do Transferegov de 09/11): 63 instrumentos de Igarassu/PE, 189 propostas, 148 termos aditivos, 1.396 mudanças de situação, 11 medições de obra. Os números em `data/` são os do último run; `data/resumo.md` é o controle.

O dado real corrigiu duas regras da primeira versão no mesmo dia: propostas aprovadas que nunca foram assinadas apareciam como "vencidas" (hoje são **NÃO ASSINADO**), e "Aguardando Prestação de Contas" era tratado como encerrado quando na verdade é um prazo correndo (hoje é **PRESTAR CONTAS**, com os dias até o limite).

## O problema

Uma secretaria municipal com ~20 instrumentos federais acompanha vigência, desbloqueios, medições e prestação de contas abrindo o Transferegov instrumento por instrumento, e registrando à mão numa planilha. A planilha atrasa, e **vigência vencida sem termo aditivo trava o repasse de uma obra**. O Transferegov publica tudo isso diariamente em CSV; ninguém no setor lia.

## Como funciona

```
Transferegov (dados abertos, CSV diário)
  proposta ──filtra COD_MUNIC_IBGE──► IDs das propostas do município
  convenio ──filtra ID_PROPOSTA────► nº dos instrumentos
  historico_situacao · termo_aditivo · prorroga_oficio · desbloqueio_cr
  desembolso · empenho · cronograma_desembolso · medições de obra
           ──filtra por nº / ID_PROPOSTA──► data/<tabela>.csv (só o município)
                                              │
                                              ▼
                      data/resumo.csv · resumo.json · resumo.md  (1 linha por instrumento)
                                              │
                               ┌──────────────┴──────────────┐
                        commit no git (sempre)        abas TG_* na planilha (opcional)
```

Decisões que importam:

- **Só o módulo "Discricionárias e Legais" tem os convênios, e ele não tem API**: são zips diários em `https://api-publica.transferegov.gestao.gov.br/downloads/dadosgov/` (licença ODbL). O script baixa cada zip inteiro e filtra em blocos (`chunksize`), porque `proposta` tem ~200 MB e `historico_situacao` ~100 MB.
- **Filtro pelo código IBGE** (`COD_MUNIC_IBGE`), com rede de segurança por UF + nome do município. Não depende de manter uma lista de instrumentos à mão; se um convênio novo for assinado, ele aparece sozinho. `instrumentos_extra` em `config.json` força a inclusão de um número específico.
- **Colunas achadas pelo nome**: se o modelo de dados do Transferegov mudar, o script para com a mensagem "a coluna X não existe (o modelo de dados mudou?)" em vez de gravar errado.
- **Saída pequena e determinística** (só o município, só colunas úteis), para o commit diário ser legível no diff.
- **A planilha é opcional e isolada**: `src/planilha.py` só escreve em abas com prefixo `TG_` e se recusa a tocar em qualquer outra. As abas mantidas por pessoas (fórmulas, responsáveis, observações) ficam intactas; quem quiser cruzar usa `XLOOKUP` apontando para `TG_RESUMO`.
- **Credenciais só em Secrets** do GitHub Actions (`GOOGLE_SA_JSON`, `PLANILHA_ID`). Sem elas, o pipeline roda só com o git.

## O que o resumo traz (uma linha por instrumento)

| Campo | De onde vem |
|---|---|
| Órgão superior, órgão, modalidade, proponente, objeto | `proposta` |
| Situação, subsituação, assinatura, vigência (atual e original), limite de prestação de contas | `convenio` |
| `DIAS_PARA_FIM` e `ALERTA_VIGENCIA` | calculado: **AMARELO** a 90 dias do fim, **VERMELHO** a 60, **VENCIDO** depois (instrumento ainda em execução); **PRESTAR CONTAS** quando a situação é "Aguardando Prestação de Contas" (aí o prazo que vale é `DIAS_PARA_PREST_CONTAS`); **ENCERRADO** para PC em análise/concluída/aprovada/rejeitada, anulado, cancelado, rescindido; **NÃO ASSINADO** para proposta aprovada que nunca virou instrumento (`IND_ASSINADO = NÃO`) |
| Valor global, repasse, contrapartida, empenhado, desembolsado, `PCT_DESEMBOLSADO`, saldo em conta | `convenio` |
| `VL_BLOQUEADO_CR` (soma do que ainda está bloqueado) | `desbloqueio_cr` |
| `QTD_TA`, `QTD_PRORROGA` | contagem em `termo_aditivo` / `prorroga_oficio` (cai para os campos do `convenio` se as tabelas vierem vazias) |
| Última situação e data | `historico_situacao` |
| Último desembolso (data e valor) | `desembolso` |
| Último boletim de medição (nº, situação, fim do período) | `acomp_obras_contratos_medicoes_modulo_empresas` (ordem numérica: o BM 10 vem depois do 9) |

`resumo.md` ordena pelo alerta (vencidos primeiro) e abre com a lista de instrumentos que vencem em 90 dias. O mesmo texto vai para o resumo da execução no GitHub Actions.

## Como usar

1. Faça um fork ou copie o repositório. Em `config.json`, coloque o `cod_munic_ibge` (7 dígitos), `municipio_nome` (como aparece no Transferegov, em maiúsculas) e `uf`.
2. Em **Settings → Actions → General**, dê permissão de escrita ao `GITHUB_TOKEN` (*Read and write permissions*), porque o workflow comita a saída.
3. Rode uma vez à mão: **Actions → Atualizar convênios → Run workflow**. Em 5 a 15 minutos, `data/` aparece com os CSVs e o `resumo.md`.
4. A partir daí roda sozinho em dia útil, 10h30 (Brasília), depois da carga diária do Transferegov.

### Planilha Google (opcional)

1. Crie uma conta de serviço no Google Cloud (API do Google Sheets ativada), baixe o JSON e **compartilhe a planilha com o e-mail da conta de serviço** como editor.
2. Em **Settings → Secrets and variables → Actions**, crie `GOOGLE_SA_JSON` (conteúdo do JSON) e `PLANILHA_ID`.
3. As abas `TG_RESUMO`, `TG_CONVENIO`, `TG_HISTORICO_SITUACAO`, … são criadas e regravadas a cada execução.

### Rodando local

```bash
pip install -r requirements.txt
python src/tg_convenios.py              # baixa (~470 MB de zips na 1ª vez, em .cache/) e gera data/
python src/tg_convenios.py --so-resumo  # refaz só o resumo a partir de data/
python -m pytest -q                     # 10 testes, sem internet
```

## Estrutura

```
src/tg_convenios.py        download, filtro em blocos, junção e resumo (lógica testada)
src/planilha.py            envio opcional para abas TG_* de uma planilha Google
tests/                     zips sintéticos no formato oficial; testa filtro, junção, alertas, saídas
.github/workflows/         execução diária + commit da saída + resumo no log
config.json                município e instrumentos extras
data/                      saída do último run (CSV por tabela, resumo.csv/json/md, _meta.json)
```

## Limitações (o que NÃO foi verificado)

- O código foi escrito sem acesso ao servidor do Transferegov e testado com dados sintéticos; a validação com dados reais é a execução no GitHub Actions. Nomes de colunas são conferidos em tempo de execução; se o modelo de dados mudar, o run falha com mensagem clara em vez de gravar lixo.
- A situação no Transferegov pode estar atrás da realidade (um TA assinado ontem aparece na carga de amanhã) ou à frente do que o setor sabe. O resumo é para conferência, não para decisão automática.
- `pagamento` (~370 MB) e `plano_aplicacao_detalhado` (~290 MB) não são baixados nesta versão.
- Os dados do Transferegov são os registrados pelo concedente/mandatária; podem atrasar em relação ao que a Caixa comunica por CE.
- Não lê anexos nem o Portal da Transparência.

## Ideias para o futuro

- Cruzar com o controle de CEs ([agente_ces_gigov](https://github.com/muryllogoncalvessilva-dotcom/agente_ces_gigov)): CE recebida × mudança de situação no Transferegov.
- Diferença entre runs no próprio `resumo.md` ("o que mudou desde ontem").
- `pagamento` filtrado para acompanhar OBTV por fornecedor.

## Fonte e licença dos dados

[Dados abertos do Transferegov.br](https://api-publica.transferegov.gestao.gov.br/) — módulo Transferências Discricionárias e Legais, arquivos CSV, licença [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/). Código deste repositório: MIT.

## Autor

Muryllo Gonçalves da Silva — [LinkedIn](https://www.linkedin.com/in/muryllocivil)
