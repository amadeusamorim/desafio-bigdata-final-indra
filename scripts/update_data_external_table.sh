#!/usr/bin/env bash
# 
# update_data_external_table.sh - Insere dados do csv no Hive
#
# Autor:      Amadeus Amorim
# Manutenção: Amadeus Amorim
#
# ------------------------------------------------------------------------ #
#   Código que irá transferir os dados dos arquivos .csv para os metadados da
#   tabela criada no HDFS, sendo a tabela externa que será referência para a 
#   a interna.
#
#  Exemplos:
#       $ ./update_data_external_table.sh
#       Neste exemplo o programa vai inserir os dados do .csv para a tabela
#     externa, de modo que fique segura no HDFS.
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

HDFS_DIR="/projeto_final/"
NOME_PASTA=$1

CIANO="\033[36;1m"
FIMCOR="\033[m"

# ------------------------------- FUNÇÕES ----------------------------------------- #

UpdateExternal () {

TABLE_NAME=$(echo "$1" | tr [a-z] [A-Z]) # Parâmetro para UPPER

echo -e "${CIANO}EFETUANDO A INGESTÃO NA TABELA ${TABLE_NAME}${FIMCOR}"
hdfs dfs -put ${TABLE_NAME}.csv ${HDFS_DIR}$1

}

# ------------------------------- EXECUÇÃO ----------------------------------------- #

cd ../dados/${NOME_PASTA}

UpdateExternal clientes
UpdateExternal divisao
UpdateExternal endereco
UpdateExternal regiao
UpdateExternal vendas