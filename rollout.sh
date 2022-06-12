#!/bin/bash

# Criando pastas no HDFS

echo "Criando HDFS de Clientes"
hdfs dfs -mkdir -p /projeto_final/clientes
echo "Criando HDFS de Divisão"
hdfs dfs -mkdir -p /projeto_final/divisao
echo "Criando HDFS de Endereço"
hdfs dfs -mkdir -p /projeto_final/endereco
echo "Criando HDFS de Regiao"
hdfs dfs -mkdir -p /projeto_final/regiao
echo "Criando HDFS de Vendas"
hdfs dfs -mkdir -p /projeto_final/vendas

# Executar o Create Tables
echo "Criando as Tabelas"
cd scripts/create_tables

echo "Tabela Clientes"
bash create_tables_clientes.sh
echo "Tabela Divisao"
bash create_tables_divisao.sh
echo "Tabela Endereco"
bash create_tables_endereco.sh
echo "Tabela Regiao"
bash create_tables_regiao.sh
echo "Tabela Vendas"
bash create_tables_vendas.sh