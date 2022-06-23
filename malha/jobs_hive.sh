#!/usr/bin/env bash
# 
# jobs_hive.sh - Malha para inserir os dados nas tabelas
#
# Autor:      Amadeus Amorim
# Manutenção: Amadeus Amorim
#
# ------------------------------------------------------------------------ #
#   Código que irá acionar os demais scripts que farão a ingestão dos dados na
#  tabela interna e externa e em seguida fará testes para certificar que os da-
#  dos foram inseridos.
#
#  Exemplos:
#       $ ./jobs_hive.sh
#       Neste exemplo o programa vai inserir os dados nas tabelas externas e in-
#  ternas.
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

AMARELO="\033[33;1m"
VERDE="\033[32;1m"

FIMCOR="\033[m"

# ------------------------------- FUNÇÕES ----------------------------------------- #

TesteTabelas () {

TABLE_NAME=$(echo "$1" | tr [a-z] [A-Z]) # Parâmetro para UPPER

echo -e "${VERDE}LISTANDO AS PARTIÇÕES ${FIMCOR}"
beeline -u jdbc:hive2://localhost:10000 -e "SHOW PARTITIONS desafio_final.TBL_${TABLE_NAME};"

echo -e "${VERDE}DESCREVENDO A TABELA ${TABLE_NAME}${FIMCOR}"
beeline -u jdbc:hive2://localhost:10000 -e "DESCRIBE desafio_final.${TABLE_NAME};"

echo -e "${VERDE}QUANTIDADE DE REGISTROS DA TABELA ${TABLE_NAME}${FIMCOR}"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) FROM desafio_final.TBL_${TABLE_NAME};"

echo -e "${VERDE}MOSTRANDO APENAS OS 10 PRIMEIROS REGISTROS${FIMCOR}"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM desafio_final.TBL_${TABLE_NAME} LIMIT 10;"
}

# ------------------------------- EXECUÇÃO ----------------------------------------- #

echo -e "${AMARELO}EFETUANDO A INGESTÃO NAS TABELAS EXTERNAS ${FIMCOR}"
bash ../scripts/update_data_external_table.sh dados_entrada

echo -e "${AMARELO}EFETUANDO A INGESTÃO NAS TABELAS WORKED ${FIMCOR}"
bash ../scripts/insert_data_worked_table.sh

TesteTabelas clientes
TesteTabelas divisao
TesteTabelas endereco
TesteTabelas regiao
TesteTabelas vendas