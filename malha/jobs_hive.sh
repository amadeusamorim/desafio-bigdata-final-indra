#!/bin/bash

# Ler Arquivo de Clientes e enviar para o HDFS
echo "Efetuando a ingestao em Clientes"
bash ../scripts/update_data_external_table_clientes.sh dados_entrada
bash ../scripts/insert_data_worked_table_clientes.sh

# Verificando as partições
echo "Listando as Partições"
beeline -u jdbc:hive2://localhost:10000 -e "SHOW PARTITIONS desafio_final.TBL_CLIENTES;"

# Descrevendo a tabela 
echo "Descrevendo a Tabela Clientes"
beeline -u jdbc:hive2://localhost:10000 -e "DESCRIBE desafio_final.TBL_CLIENTES;"

# Count na tabela
echo "Quantidade de Registros da Tabela"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) FROM desafio_final.TBL_CLIENTES;"

# Mostrando as 10 primeiras linhas da tabela
echo "Mostrando Apenas os 10 primeiros"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM desafio_final.TBL_CLIENTES LIMIT 10;"

##########################

# Ler Arquivo de Divisao e enviar para o HDFS
echo "Efetuando a ingestao em Divisao"
bash ../scripts/update_data_external_table_divisao.sh dados_entrada
bash ../scripts/insert_data_worked_table_divisao.sh

# Verificando as partições
echo "Listando as Partições"
beeline -u jdbc:hive2://localhost:10000 -e "SHOW PARTITIONS desafio_final.TBL_DIVISAO;"

# Descrevendo a tabela 
echo "Descrevendo a Tabela Divisao"
beeline -u jdbc:hive2://localhost:10000 -e "DESCRIBE desafio_final.TBL_DIVISAO;"

# Count na tabela
echo "Quantidade de Registros da Divisao"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) FROM desafio_final.TBL_DIVISAO;"

# Mostrando as 10 primeiras linhas da tabela
echo "Mostrando Apenas os 10 primeiros"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM desafio_final.TBL_DIVISAO LIMIT 10;"

##########################

# Ler Arquivo de Endereco e enviar para o HDFS
echo "Efetuando a ingestao em Endereco"
bash ../scripts/update_data_external_table_endereco.sh dados_entrada
bash ../scripts/insert_data_worked_table_endereco.sh

# Verificando as partições
echo "Listando as Partições"
beeline -u jdbc:hive2://localhost:10000 -e "SHOW PARTITIONS desafio_final.TBL_ENDERECO;"

# Descrevendo a tabela 
echo "Descrevendo a Tabela Endereco"
beeline -u jdbc:hive2://localhost:10000 -e "DESCRIBE desafio_final.TBL_ENDERECO;"

# Count na tabela
echo "Quantidade de Registros da Endereco"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) FROM desafio_final.TBL_ENDERECO;"

# Mostrando as 10 primeiras linhas da tabela
echo "Mostrando Apenas os 10 primeiros"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM desafio_final.TBL_ENDERECO LIMIT 10;"

##########################

# Ler Arquivo de Regiao e enviar para o HDFS
echo "Efetuando a ingestao em Regiao"
bash ../scripts/update_data_external_table_regiao.sh dados_entrada
bash ../scripts/insert_data_worked_table_regiao.sh

# Verificando as partições
echo "Listando as Partições"
beeline -u jdbc:hive2://localhost:10000 -e "SHOW PARTITIONS desafio_final.TBL_REGIAO;"

# Descrevendo a tabela 
echo "Descrevendo a Tabela Regiao"
beeline -u jdbc:hive2://localhost:10000 -e "DESCRIBE desafio_final.TBL_REGIAO;"

# Count na tabela
echo "Quantidade de Registros da Regiao"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) FROM desafio_final.TBL_REGIAO;"

# Mostrando as 10 primeiras linhas da tabela
echo "Mostrando Apenas os 10 primeiros"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM desafio_final.TBL_REGIAO LIMIT 10;"

##########################

# Ler Arquivo de Vendas e enviar para o HDFS
echo "Efetuando a ingestao em Vendas"
bash ../scripts/update_data_external_table_vendas.sh dados_entrada
bash ../scripts/insert_data_worked_table_vendas.sh

# Verificando as partições
echo "Listando as Partições"
beeline -u jdbc:hive2://localhost:10000 -e "SHOW PARTITIONS desafio_final.TBL_VENDAS;"

# Descrevendo a tabela 
echo "Descrevendo a Tabela Vendas"
beeline -u jdbc:hive2://localhost:10000 -e "DESCRIBE desafio_final.TBL_VENDAS;"

# Count na tabela
echo "Quantidade de Registros da Vendas"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) FROM desafio_final.TBL_VENDAS;"

# Mostrando as 10 primeiras linhas da tabela
echo "Mostrando Apenas os 10 primeiros"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM desafio_final.TBL_VENDAS LIMIT 10;"