#!/bin/bash
# Drop tabelas

echo -e "\n--- Deletando pastas do HDFS ---\n"
hdfs dfs -rm -r /projeto_final/

echo -e "\n--- Deletando tabelas Hive ---\n"

echo "Deletando tabela de CLIENTES"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_CLIENTES;"
echo "Deletando tabela de CLIENTES_EXTERNA"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_CLIENTES_STG;"
echo "Deletando tabela de DIVISAO"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_DIVISAO;"
echo "Deletando tabela de DIVISAO_EXTERNAL"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_DIVISAO_STG;"
echo "Deletando tabela de ENDERECO"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_ENDERECO;"
echo "Deletando tabela de ENDERECO_EXTERNAL"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_ENDERECO_STG;"
echo "Deletando tabela de REGIAO"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_REGIAO;"
echo "Deletando tabela de REGIAO_EXTERNAL"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_REGIAO_STG;"
echo "Deletando tabela de VENDAS"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_VENDAS;"
echo "Deletando tabela de VENDAS_EXTERNAL"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_VENDAS_STG;"
echo "Deletando banco do desafio"
beeline -u jdbc:hive2://localhost:10000 -e "DROP DATABASE IF EXISTS desafio_final;"