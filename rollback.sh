#!/bin/bash
# Drop tabelas
hdfs dfs -rm -r /projeto_final/

# Drop HDFS
echo "Deletanto tabela de CLIENTES"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_CLIENTES;"
echo "Deletanto tabela de CLIENTES_EXTERNA"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_CLIENTES_STG;"
echo "Deletanto tabela de DIVISAO"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_DIVISAO;"
echo "Deletanto tabela de DIVISAO_EXTERNAL"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_DIVISAO_STG;"
echo "Deletanto tabela de ENDERECO"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_ENDERECO;"
echo "Deletanto tabela de ENDERECO_EXTERNAL"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_ENDERECO_STG;"
echo "Deletanto tabela de REGIAO"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_REGIAO;"
echo "Deletanto tabela de REGIAO_EXTERNAL"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_REGIAO_STG;"
echo "Deletanto tabela de VENDAS"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_VENDAS;"
echo "Deletanto tabela de VENDAS_EXTERNAL"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_VENDAS_STG;"
echo "Deletanto banco do desafio"
beeline -u jdbc:hive2://localhost:10000 -e "DROP DATABASE IF EXISTS desafio_final;"