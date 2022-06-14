#!/bin/bash

# Criando pastas no HDFS

echo -e "\n--- Criando Pastas de Entradas no HDFS ---\n"

echo "Pasta de Clientes - HDFS"
hdfs dfs -mkdir -p /projeto_final/clientes
echo "Pasta de Divisão - HDFS"
hdfs dfs -mkdir -p /projeto_final/divisao
echo "Pasta de Endereço - HDFS"
hdfs dfs -mkdir -p /projeto_final/endereco
echo "Pasta de Região - HDFS"
hdfs dfs -mkdir -p /projeto_final/regiao
echo "Pasta de Vendas - HDFS"
hdfs dfs -mkdir -p /projeto_final/vendas

echo -e "\n--- Criando Pasta Staging no HDFS ---\n"
hdfs dfs -mkdir -p /projeto_final/staging

echo -e "\n--- Criando Pastas para Dimensão e FATO no HDFS ---\n"
echo "Pasta de Dimensão Clientes - HDFS"
hdfs dfs -mkdir -p /projeto_final/dados_saida/dimclientes
echo "Pasta de Dimensão Localidade - HDFS"
hdfs dfs -mkdir -p /projeto_final/dados_saida/dimlocalidade
echo "Pasta de Dimensão Produtos - HDFS"
hdfs dfs -mkdir -p /projeto_final/dados_saida/dimprodutos
echo "Pasta de Dimensão Tempo - HDFS"
hdfs dfs -mkdir -p /projeto_final/dados_saida/dimtempo
echo "Pasta de FATO Vendas - HDFS"
hdfs dfs -mkdir -p /projeto_final/dados_saida/fatovendas

# Executar o Create Tables
echo -e "\n--- Criando Tabelas no Hive ---\n"
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