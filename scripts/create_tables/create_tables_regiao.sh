#!/bin/bash

HDFS_DIR="/projeto_final/regiao"
TARGET_DATABASE="desafio_final"
TARGET_TABLE_EXTERNAL="TBL_REGIAO_STG"
TARGET_TABLE="TBL_REGIAO"
DT_FOTO="$(date --date="-1 day" "+%Y-%m-%d")"

# Script usado para criaçao das tabelas

# CRIACAO DA TABELA EXTERNA
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar HDFS_DIR="${HDFS_DIR}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE_EXTERNAL}"\
 -f ../../hqls/hql-regiao/create-external-table-regiao-stg.hql 

# CRIACAO DA TABELA WORKED
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE}" \
 -f ../../hqls/hql-regiao/create-managed-table-regiao-wrk.hql root@hive_server:/input/desafio-bigdata-final-indra/scripts/create_tables# 