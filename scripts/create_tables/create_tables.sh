#!/usr/bin/env bash
# 
# create_tables.sh - Cria tabelas Internas e Externas no Hive
#
# Autor:      Amadeus Amorim
# Manutenção: Amadeus Amorim
#
# ------------------------------------------------------------------------ #
#   Código que irá criar as tabelas Externas (inserindo os metadados) e Internas
# com as respectivas partições. O código em questão irá acionar as tabelas 
#
#  Exemplos:
#       $ ./create_tables.sh
#       Neste exemplo o programa vai criar as tabelas necessárias, antes do script 
#       da malha que fazerá a inserção dos dados do csv;
#
# ------------------------------------------------------------------------ #
# Histórico:
#
#   v1.0 12/06/2022, Amadeus:
#     - Criação de vários arquivos create tables para cada tabela
#   v1.1 22/06/2022, Amadeus:
#     - Adicionando comentários e identando o código
#     - Adicionando variáveis
#     - Adicionando funções
#     - Adicionando parâmetros
# ------------------------------------------------------------------------ #
# Testado em:
#   bash 5.0.17(1)-release
# --------------------------- VARIÁVEIS ---------------------------------- #

HDFS_DIR="/projeto_final/"
TARGET_DATABASE="desafio_final"
TARGET_TABLE="TBL_"
DT_FOTO="$(date --date="-1 day" "+%Y-%m-%d")"  # Partição com data de ontem

AMARELO="\033[33;1m"
AZUL="\033[34;1m"
FIMCOR="\033[m"

# ------------------------------- TESTES ----------------------------------------- #
# [ ! -e "$ARQUIVO_BANCO_DE_DADOS" ] && echo "ERRO. Arquivo não existe." && exit 1
# [ ! -r "$ARQUIVO_BANCO_DE_DADOS" ] && echo "ERRO. Arquivo não tem permissão de leitura." && exit 1
# [ ! -w "$ARQUIVO_BANCO_DE_DADOS" ] && echo "ERRO. Arquivo não tem permissão de escrita." && exit 1
# ------------------------------------------------------------------------ #

# ------------------------------- FUNÇÕES ----------------------------------------- #

CriaTabelas () {

TABLE_NAME=$(echo "$1" | tr [a-z] [A-Z]) # Parâmetro para UPPER

echo -e "${AMARELO}CRIANDO TABELA ${TABLE_NAME} EXTERNA NO HIVE${FIMCOR}"
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE=${TARGET_DATABASE}\
 --hivevar HDFS_DIR=${HDFS_DIR}$1\
 --hivevar TARGET_TABLE=${TARGET_TABLE}${TABLE_NAME}_STG\
 -f ../../hqls/hql-$1/create-external-table-$1-stg.hql 

echo -e "${AZUL}CRIANDO TABELA ${TABLE_NAME} WORKED NO HIVE${FIMCOR}"
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE=${TARGET_DATABASE}\
 --hivevar TARGET_TABLE=${TARGET_TABLE}${TABLE_NAME}\
 -f ../../hqls/hql-$1/create-managed-table-$1-wrk.hql root@hive_server:/input/desafio-bigdata-final-indra/scripts/create_tables# 
}

# ------------------------------- EXECUÇÃO ----------------------------------------- #

CriaTabelas clientes
CriaTabelas divisao
CriaTabelas endereco
CriaTabelas regiao
CriaTabelas vendas

