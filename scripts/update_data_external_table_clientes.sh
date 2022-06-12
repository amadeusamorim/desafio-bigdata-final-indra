
HDFS_DIR="/projeto_final/clientes"
NOME_PASTA=$1

echo "Efetuando a ingestão na tabela de Clientes"
cd ../dados/${NOME_PASTA}
hdfs dfs -copyFromLocal CLIENTES.csv ${HDFS_DIR}
