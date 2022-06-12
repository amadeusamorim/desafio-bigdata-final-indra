
HDFS_DIR="/projeto_final/regiao"
NOME_PASTA=$1

echo "Efetuando a ingestão na tabela de Regiao"
cd ../dados/${NOME_PASTA}
hdfs dfs -copyFromLocal REGIAO.csv ${HDFS_DIR}
