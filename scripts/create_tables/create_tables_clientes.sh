#!/bin/bash

HDFS_DIR="/projeto_final/clientes"
TARGET_DATABASE="desafio_final"
TARGET_TABLE_EXTERNAL="TBL_CLIENTES_STG"
TARGET_TABLE="TBL_CLIENTES"
DT_FOTO="$(date --date="-1 day" "+%Y-%m-%d")"


# Script usado para criaçao das tabelas

# CRIACAO DA TABELA EXTERNA
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar HDFS_DIR="${HDFS_DIR}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE_EXTERNAL}"\
 -f ../../hqls/hql-clientes/create-external-table-clientes-stg.hql 

# CRIACAO DA TABELA WORKED
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE}" \
 -f ../../hqls/hql-clientes/create-managed-table-clientes-wrk.hql root@hive_server:/input/desafio-bigdata-final-indra/scripts/create_tables# 