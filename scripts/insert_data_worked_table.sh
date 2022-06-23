#!/usr/bin/env bash
# 
# insert_data_worked_table.sh - Insere dados nas tabelas internas
#
# Autor:      Amadeus Amorim
# Manutenção: Amadeus Amorim
#
# ------------------------------------------------------------------------ #
#   Código que irá inserir dados e partição na tabela Interna, essa tabela 
#   será manuseada pelo Spark.
#
#  Exemplos:
#       $ ./insert_data_worked_table.sh
#       Neste exemplo o programa vai inserir os dados da tabela externa para
#     a interna, de modo que os dados possam ser tratados.
#
# ------------------------------------------------------------------------ #
# Histórico:
#
#   v1.0 12/06/2022, Amadeus:
#     - Criação de arquivos inserts individuais para cada tabela interna
#   v1.1 22/06/2022, Amadeus:
#     - Adicionando comentários e identando o código
#     - Adicionando variáveis
#     - Adicionando funções
#     - Adicionando parâmetros
# ------------------------------------------------------------------------ #
# Testado em:
#   bash 5.0.17(1)-release
# --------------------------- VARIÁVEIS ---------------------------------- #

TARGET_DATABASE="desafio_final"
TABLE="TBL_"
STAGE_DATABASE="desafio_final"
DT_FOTO="$(date "+%Y-%m-%d")"

# ------------------------------- FUNÇÕES ----------------------------------------- #

InsereDados () {

TABLE_NAME=$(echo "$1" | tr [a-z] [A-Z]) # Parâmetro para UPPER

beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar TARGET_TABLE="${TABLE}${TABLE_NAME}" \
 --hivevar STAGE_TABLE="${TABLE}${TABLE_NAME}_STG" \
 --hivevar STAGE_DATABASE="${STAGE_DATABASE}" \
 --hivevar DT_FOTO="${DT_FOTO}" \
 -f ../hqls/hql-$1/insert-table-$1-wrk.hql 
}

# ------------------------------- EXECUÇÃO ----------------------------------------- #

InsereDados clientes
InsereDados divisao
InsereDados endereco
InsereDados regiao
InsereDados vendas