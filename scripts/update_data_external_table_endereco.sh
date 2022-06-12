
HDFS_DIR="/projeto_final/endereco"
NOME_PASTA=$1

echo "Efetuando a ingestão na tabela de Endereco"
cd ../dados/${NOME_PASTA}
hdfs dfs -copyFromLocal ENDERECO.csv ${HDFS_DIR}
