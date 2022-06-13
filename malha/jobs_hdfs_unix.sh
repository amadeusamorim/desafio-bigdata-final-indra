#!/bin/bash

HDFS_DIRECTORY="/projeto_final/dados_saida/"

echo "Direcionando-se para pastas dados_saida"
cd ../dados/dados_saida

echo "Removendo todos os arquivos antigos"
rm *

echo "Trazendo os arquivos do hdfs"
hdfs dfs -get ${HDFS_DIRECTORY}dimclientes/*
hdfs dfs -get ${HDFS_DIRECTORY}dimlocalidade/*
hdfs dfs -get ${HDFS_DIRECTORY}dimprodutos/*
hdfs dfs -get ${HDFS_DIRECTORY}dimtempo/*
hdfs dfs -get ${HDFS_DIRECTORY}fatovendas/*

echo "Importaçao realizada com sucesso"