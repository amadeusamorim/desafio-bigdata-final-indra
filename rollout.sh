#!/usr/bin/env bash
# 
# rollout.sh - Cria pastas no HDFS e aciona o script para criar tabelas
#
# Autor:      Amadeus Amorim
# Manutenção: Amadeus Amorim
#
# ------------------------------------------------------------------------ #
#   Irá criar pastas de entradas, para inserção das tabelas .csv "raw", além de 
# criar uma pasta staging, para que os dados possam ser colocados lá enquanto 
# são tratados e também cria as pastas no HDFS para as tabelas DIM e FATO.
#   O rollout.sh também acionará o script create_tables.sh para que possa
# assim criar as tabelas internas e externas do Hive.
# em maiúsculo e em ordem alfabética.
#
#  Exemplos:
#       $ ./rollout.sh
#       Neste exemplo o programa vai criar as pastas e tabelas necessárias que 
#       dão início a pipeline.
# ------------------------------------------------------------------------ #
# Histórico:
#
#   v1.0 12/06/2022, Amadeus:
#     - Criação do rollout sem variáveis com arquivos à serem lidos de forma distinta
#   v1.1 21/06/2022, Amadeus:
#     - Adicionando comentários e identando o código
#     - Adicionando variáveis
#   v1.2 22/06/2022, Amadeus:
#     - Adicionando funções
#     - Adicionando parâmetros
#     - Adicionando menu e help
# ------------------------------------------------------------------------ #
# Testado em:
#   bash 5.0.17(1)-release
# --------------------------- VARIÁVEIS ---------------------------------- #

ENTRADA_HDFS="/projeto_final/"
SAIDA_HDFS="/projeto_final/dados_saida/"
STAGE_HDFS="/projeto_final/staging/"
MENSAGEM_USO="
  $(basename $0) - [OPÇÕES]

    -i - Cria pasta para tabelas raw (input) | Aguarda parâmetro com nome da pasta.
    -o - Cria pasta para tabelas dimensão (output) | Aguarda parâmetro com nome da pasta.
    -s - Cria pasta staginig
    -h - Menu de ajuda
"
AMARELO="\033[33;1m"
AZUL="\033[34;1m"

# ------------------------------------------------------------------------ #

# ------------------------------- FUNÇÕES ----------------------------------------- #

CriaPasta () {
  case "$1" in
    -i) hdfs dfs -mkdir -p "${ENTRADA_HDFS}$2"        ;;
    -o) hdfs dfs -mkdir -p "${SAIDA_HDFS}$2"          ;;
    -s) hdfs dfs -mkdir -p "${STAGE_HDFS}"            ;;
    -h) echo "$MENSAGEM_USO" && exit 0                ;;
     *) echo "Opção Inválida, valide o -h." && exit 1 ;;
  esac
}

# ------------------------------- EXECUÇÃO ----------------------------------------- #

echo -e "${AMARELO}CRIANDO PASTAS DE ENTRADAS NO HDFS"${FIMCOR}
CriaPasta -i clientes
CriaPasta -i divisao
CriaPasta -i endereco
CriaPasta -i regiao
CriaPasta -i vendas

echo -e "${AMARELO}CRIANDO PASTA STAGING NO HDFS${FIMCOR}"
CriaPasta -s

echo -e "${AMARELO}CRIANDO PASTAS DE SAÍDA NO HDFS${FIMCOR}"
CriaPasta -o dimclientes
CriaPasta -o dimlocalidade
CriaPasta -o dimprodutos
CriaPasta -o dimtempo
CriaPasta -o fatovendas

echo -e "${AZUL}CRIANDO TABELAS NO HIVE${FIMCOR}"
cd scripts/create_tables 
bash create_tables.sh # Executa script para criar tabelas
