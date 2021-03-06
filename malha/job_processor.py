from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import os
import re

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# Criando dataframes diretamente do Hive
df_clientes = spark.sql("SELECT * FROM desafio_final.TBL_CLIENTES")
df_divisao = spark.sql("SELECT * FROM desafio_final.TBL_DIVISAO")
df_endereco = spark.sql("SELECT * FROM desafio_final.TBL_ENDERECO")
df_regiao = spark.sql("SELECT * FROM desafio_final.TBL_REGIAO")
df_vendas = spark.sql("SELECT * FROM desafio_final.TBL_VENDAS")

# Retirando todos os espaços das colunas dos dataframes
for each in df_clientes.schema.names:
    df_clientes = df_clientes.withColumnRenamed(each,  re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*','',each.replace(' ', '_')))
    
for each in df_divisao.schema.names:
    df_divisao = df_divisao.withColumnRenamed(each,  re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*','',each.replace(' ', '_')))
    
for each in df_endereco.schema.names:
    df_endereco = df_endereco.withColumnRenamed(each,  re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*','',each.replace(' ', '_')))
    
for each in df_regiao.schema.names:
    df_regiao = df_regiao.withColumnRenamed(each,  re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*','',each.replace(' ', '_')))
        
for each in df_vendas.schema.names:
    df_vendas = df_vendas.withColumnRenamed(each,  re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*','',each.replace(' ', '_')))

# Tratando linha line_of_business que estava com três espaços '   ' para 'Nao informado' por ser string.
df_clientes = df_clientes.withColumn('line_of_business', regexp_replace('line_of_business', '   ', 'Nao informado'))

# Eliminando a coluna dt_foto
df_clientes = df_clientes.drop('dt_foto')

# Removendo duplicidades da PK do DF Clientes
df_clientes = df_clientes.dropDuplicates(["customerkey"])

# Apagando a coluna dt_foto do DF Divisão
df_divisao = df_divisao.drop('dt_foto')

# Apagando a coluna dt_foto do DF Endereço
df_endereco = df_endereco.drop('dt_foto')

# Tratando as colunas sujas com espaço em branco e passando a informação de 'Nao informado' para as colunas vazias após tratamento
for column in df_endereco.columns:
    df_endereco = df_endereco.withColumn(column, trim(df_endereco[column]))
    df_endereco = df_endereco.withColumn(column, when(df_endereco[column] == '', "Nao informado")\
                                         .when(df_endereco[column].isNull(), "Nao informado")\
                                         .otherwise(df_endereco[column]))
    
df_endereco = df_endereco.withColumn("address_number", col("address_number").cast('integer'))

# Removendo duplicidades da PK de Endereço
df_endereco = df_endereco.dropDuplicates(["address_number"])

# Apagando a coluna dt_foto do DF Região
df_regiao = df_regiao.drop('dt_foto')

# Apagando a coluna dt_foto do DF Vendas
df_vendas = df_vendas.drop('dt_foto')

# Retirando as linhas que são nulas em completo da tabela
df_vendas = df_vendas.filter(col("customerkey").isNotNull())

# Tratando dados com espaços em branco nas rows (todos são Strings) e tidos como "NotNull"
for c in df_vendas.columns:
    df_vendas = df_vendas.withColumn(c, trim(df_vendas[c]))
    df_vendas = df_vendas.withColumn(c, when(df_vendas[c] == '', 'Nao informado')\
                                         .otherwise(df_vendas[c]))

# Alterando os tipos das colunas para os seus respectivos e corretos tipos
# Usando o dtypes que retorna o o nome da coluna e o tipo
for i in df_vendas.dtypes:
    if re.search("date", i[0]):
        df_vendas = df_vendas.withColumn(i[0],to_date(col(i[0]), 'dd/MM/yyyy').alias(i[0]))

vendastonumber = ['discount_amount', 'list_price', 'sales_amount', 
                'sales_amount_based_on_list_price', 'sales_cost_amount',
                 'sales_margin_amount', 'sales_price', 'sales_quantity']

for c in vendastonumber:
    df_vendas = df_vendas.withColumn(c, regexp_replace(c, '\,', '.'))
    if c == 'sales_amount' or c == 'sales_quantity':
        df_vendas = df_vendas.withColumn(c, col(c).cast('integer'))
    else:
        df_vendas = df_vendas.withColumn(c, col(c).cast('double'))
        df_vendas = df_vendas.withColumn(c, round(c, 2))

# Realizando for para checar se a coluna é String e repassando parâmetros 
# Caso seja int ou double, preencher com 0, caso seja string preencher com Nao informado

for i in df_vendas.dtypes:
    if re.search("int", i[1]) or re.search("double", i[1]):
        df_vendas = df_vendas.withColumn(i[0], when(df_vendas[i[0]].isNull(), 0).otherwise(df_vendas[i[0]]))  
    elif re.search("string", i[1]):
        df_vendas = df_vendas.withColumn(i[0], when(df_vendas[i[0]].isNull(), 'Nao informado').otherwise(df_vendas[i[0]]))

### CóDIGOS ABAIXO REFERENTES AS TABELAS DIMENSIONAL E FATO ###

# Criando DF Dim_Tempo (Precisa ser criada pois não dá para adaptar das demais planilhas)
dim_tempo = spark.createDataFrame([(1,)], ["id"])

# Iniciando sequência com o primeiro dia da DateKey até a daa atual
dim_tempo = dim_tempo.withColumn("date", f.explode(f.expr("sequence(to_date('2017-01-09'), to_date(current_date()), interval 1 day)"))
)

# Adicionando mais colunas no Dim_Tempo e eliminando a 'id'

# Eliminando a coluna ID pois ela só foi criada para gerar o DF
dim_tempo = dim_tempo.drop('id')

# Formatando o nome da minha coluna 'date' para 'DATA'
dim_tempo = dim_tempo.withColumn('DATA', f.col('date'))

# Fatiando o ano para Inteiro e uma coluna em separado
dim_tempo = dim_tempo.withColumn('NR_ANO', f.date_format(f.col('date'), 'yyyy').cast(IntegerType()))

# Fatiando o ms para Inteiro e uma coluna em separado
dim_tempo = dim_tempo.withColumn('NR_MES', f.date_format(f.col('date'), 'MM').cast(IntegerType()))

# Atribuindo o nome dos trimestres de acordo com os meses
dim_tempo = dim_tempo.withColumn('NM_TRIMESTRE', when(dim_tempo.NR_MES <= 3, "1º Trimestre")\
                                 .when(dim_tempo.NR_MES <= 6, "2º Trimestre")\
                                 .when(dim_tempo.NR_MES <= 9, "3º Trimestre")\
                                 .when(dim_tempo.NR_MES <= 12, "4º Trimestre"))

# Atribuindo o nome dos meses de acordo com o número dos meses
dim_tempo = dim_tempo.withColumn('NM_MES', when(dim_tempo.NR_MES == 1, "Janeiro")\
                                 .when(dim_tempo.NR_MES == 2, "Fevereiro")\
                                 .when(dim_tempo.NR_MES == 3, "Março")\
                                 .when(dim_tempo.NR_MES == 4, "Abril")\
                                 .when(dim_tempo.NR_MES == 5, "Maio")\
                                 .when(dim_tempo.NR_MES == 6, "Junho")\
                                 .when(dim_tempo.NR_MES == 7, "Julho")\
                                 .when(dim_tempo.NR_MES == 8, "Agosto")\
                                 .when(dim_tempo.NR_MES == 9, "Setembro")\
                                 .when(dim_tempo.NR_MES == 10, "Outubro")\
                                 .when(dim_tempo.NR_MES == 11, "Novembro")\
                                 .when(dim_tempo.NR_MES == 12, "Dezembro"))

# Pegando os dias da semana e em seguida atribuindo nome ao número relativos a esses dias em outra coluna
dim_tempo = dim_tempo.withColumn('NR_DIA_SEMANA', dayofweek(col('DATA')))
dim_tempo = dim_tempo.withColumn('NM_DIA_SEMANA', when(dim_tempo.NR_DIA_SEMANA == 1, "Domingo")\
                                 .when(dim_tempo.NR_DIA_SEMANA == 2, "Segunda-feira")\
                                 .when(dim_tempo.NR_DIA_SEMANA == 3, "Terça-feira")\
                                 .when(dim_tempo.NR_DIA_SEMANA == 4, "Quarta-feira")\
                                 .when(dim_tempo.NR_DIA_SEMANA == 5, "Quinta-feira")\
                                 .when(dim_tempo.NR_DIA_SEMANA == 6, "Sexta-feira")\
                                 .when(dim_tempo.NR_DIA_SEMANA == 7, "Sábado"))

# Definindo os números da semana no ano
dim_tempo = dim_tempo.withColumn('NR_SEMANA', weekofyear(col('DATA')))

# Definindo as colunas do dia, derivando da coluna 'date' e tranformando em Inteiro
dim_tempo = dim_tempo.withColumn('NR_DIA', f.date_format(f.col('date'), 'dd').cast(IntegerType()))

# Jogando fora a coluna date pois DATA a substituiu
dim_tempo = dim_tempo.drop('date')

# Reordenando e ajustando a dimensão Tempo
dim_tempo = dim_tempo.select('DATA', 'NR_ANO', 'NM_TRIMESTRE', 'NM_MES', 'NR_SEMANA'
                             , 'NR_DIA', 'NM_DIA_SEMANA', 'NR_DIA_SEMANA')

# Realizando o Join entre Endereço, Divisao e Regiao e fazendo Select das colunas que vou utilizar
stg_cliente = df_clientes.join(df_endereco, df_clientes.address_number == df_endereco.address_number, 'left').join(df_divisao, df_clientes.division == df_divisao.division, 'inner').join(df_regiao, df_clientes.region_code == df_regiao.region_code, 'inner').select(df_clientes.address_number, df_endereco.city, df_endereco.state, df_divisao.division_name, df_regiao.region_name, df_clientes.customerkey, df_clientes.customer, df_clientes.
customer_type)

# Preenchendo Cidade e Estado com "Nao informado" de modo que as demais informações nao se percam, tais como produtos vendidos e também valores faturados
stg_cliente = stg_cliente.na.fill('Nao informado')

# Realizando o Join da stg_cliente com a df_vendas
df_vendas_stg = df_vendas.join(stg_cliente, df_vendas.customerkey == stg_cliente.customerkey, 'inner').join(dim_tempo, df_vendas.datekey == dim_tempo.DATA).select(stg_cliente.address_number, stg_cliente.city, stg_cliente.state, stg_cliente.division_name, stg_cliente.region_name, df_vendas.customerkey, stg_cliente.customer, stg_cliente.customer_type, df_vendas.item_number, df_vendas.item, df_vendas.sales_margin_amount, df_vendas.sales_amount, df_vendas.sales_cost_amount, df_vendas.sales_quantity, df_vendas.item_class, df_vendas.datekey, dim_tempo.NR_ANO, dim_tempo.NM_TRIMESTRE, dim_tempo.NM_MES, dim_tempo.NR_DIA, dim_tempo.NM_DIA_SEMANA, dim_tempo.NR_DIA_SEMANA, dim_tempo.NR_SEMANA)

# Criando a SK de produto com Hash das colunas de suas dimensões
df_vendas_stg = df_vendas_stg.withColumn("SK_PRODUTO", sha2(concat_ws("", df_vendas_stg.item_number, df_vendas_stg.item, df_vendas_stg.item_class), 256))

# Criando as demais hashs
df_vendas_stg = df_vendas_stg.withColumn("SK_CLIENTE", sha2(concat_ws("", df_vendas_stg.customerkey, df_vendas_stg.customer, df_vendas_stg.city, df_vendas_stg.customer_type), 256))

df_vendas_stg = df_vendas_stg.withColumn("SK_LOCALIDADE", sha2(concat_ws("", df_vendas_stg.address_number, df_vendas_stg.city, df_vendas_stg.state, df_vendas_stg.region_name, df_vendas_stg.division_name), 256))

df_vendas_stg = df_vendas_stg.withColumn("SK_DATA", sha2(concat_ws("", df_vendas_stg.datekey, df_vendas_stg.NR_ANO, df_vendas_stg.NM_TRIMESTRE, df_vendas_stg.NM_MES, df_vendas_stg.NR_DIA, df_vendas_stg.NM_DIA_SEMANA, df_vendas_stg.NR_DIA_SEMANA, df_vendas_stg.NR_SEMANA), 256))

# Selecionando as colunas da minha Dimensão Cliente da tabela staging
dim_clientes = df_vendas_stg.select('SK_CLIENTE', 'customerkey', 'customer', 'customer_type', 'city')

# Renomeando as colunas
dim_clientes = dim_clientes.withColumnRenamed("customerkey","NK_ID_CLIENTE")\
                            .withColumnRenamed("customer", "NM_CLIENTE")\
                            .withColumnRenamed("customer_type", "DESC_TIPO_CLIENTE")\
                            .withColumnRenamed("city", "NM_CIDADE_CLIENTE")

# Eliminando colunas nulas
dim_clientes = dim_clientes.na.drop()

# Eliminando duplicidades da SK_CLIENTE (Distinct)
dim_clientes = dim_clientes.dropDuplicates(["SK_CLIENTE"])

# Selecionando as colunas da minha Dimensão Produto da tabela staging
dim_produtos = df_vendas_stg.select('SK_PRODUTO', 'item_number', 'item', 'item_class')

# Renomeando colunas
dim_produtos = dim_produtos.withColumnRenamed("item_number","NK_ID_PRODUTO")\
                            .withColumnRenamed("item", "NM_PRODUTO")\
                            .withColumnRenamed("item_class", "NM_CATEGORIA_PRODUTO")

# Eliminando colunas nulas
dim_produtos = dim_produtos.na.drop()

# Eliminando duplicidades da SK_PRODUTO (Distinct)
dim_produtos = dim_produtos.dropDuplicates(["SK_PRODUTO"])

# Selecionando as colunas da minha Dimensão Localidade da tabela staging    
dim_localidade = df_vendas_stg.select('SK_LOCALIDADE', 'address_number', 'city', 'state', 'division_name', 'region_name')

# Renomeando colunas
dim_localidade = dim_localidade.withColumnRenamed("address_number","NK_ID_LOCALIDADE")\
                            .withColumnRenamed("city", "NM_CIDADE_LOCALIDADE")\
                            .withColumnRenamed("state", "NM_ESTADO_LOCALIDADE")\
                            .withColumnRenamed("division_name", "NM_DIVISAO_LOCALIDADE")\
                            .withColumnRenamed("region_name", "NM_REGIAO_LOCALIDADE")

# Eliminando colunas nulas
dim_localidade = dim_localidade.na.drop()

# Eliminando duplicidades da SK_PRODUTO (Distinct)
dim_localidade = dim_localidade.dropDuplicates(["SK_LOCALIDADE"])

# Selecionando as colunas da minha Dimensão Tempo da tabela staging    
# Selecionando as colunas da minha Dimensão Tempo da tabela staging    
dim_tempo = df_vendas_stg.select('SK_DATA', 'datekey', 'NR_ANO', 'NM_TRIMESTRE', 'NM_MES', 'NR_SEMANA', 'NR_DIA', 'NM_DIA_SEMANA', 'NR_DIA_SEMANA')

# Renomeando coluna datekey
dim_tempo = dim_tempo.withColumnRenamed("datekey", "DATA")

# Eliminando possíveis nulas
dim_tempo = dim_tempo.na.drop()

# Eliminando duplicidades da SK_PRODUTO (Distinct)
dim_tempo = dim_tempo.dropDuplicates(["SK_DATA"])

# Renomeando coluna datekey
dim_tempo = dim_tempo.withColumnRenamed("datekey", "DATA")

# Eliminando possíveis nulas
dim_tempo = dim_tempo.na.drop()

# Eliminando duplicidades da SK_PRODUTO (Distinct)
dim_tempo = dim_tempo.dropDuplicates(["SK_DATA"])

# Implementando a tabela FATO VENDAS usando SQL (facilidade na agregação)

df_vendas_stg.createOrReplaceTempView('stg_vendas')

ft_vendas = spark.sql(""" SELECT SK_CLIENTE, SK_PRODUTO, SK_LOCALIDADE, SK_DATA
                        , COUNT(item) AS QTD_VENDAS, SUM(sales_amount) AS VL_VENDAS
                        , SUM(sales_margin_amount) AS MARGEM_VENDAS, SUM(sales_quantity) AS PRODUTOS_VENDIDOS
                        FROM stg_vendas
                        GROUP BY SK_CLIENTE, SK_PRODUTO, SK_LOCALIDADE, SK_DATA
                        ORDER BY QTD_VENDAS DESC""")

# Criando job para salvar meus arquivos que serão baixados do HDFS e renomea-los já com o nome corretos
# A varivel output diz a pasta onde ela deve ser movida após a stage
# A variável erase apaga arquivos no output, caso ja exista, para que nao haja choque de mesmo nome (exemplo de overwrite)
# A variavel rename, além de mover, renomer o arquivo entre uma pasta e outra
def salvar_df(df, file):
    output = "/projeto_final/dados_saida/" + file
    erase = "hdfs dfs -rm " + output + "/*" 
    rename = "hdfs dfs -mv /projeto_final/staging/part-*" + ' ' + output + '/' + file + ".csv"
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/projeto_final/staging/")

    os.system(erase)
    os.system(rename)

salvar_df(dim_clientes, 'dimclientes')
salvar_df(dim_produtos, 'dimprodutos')
salvar_df(dim_localidade, 'dimlocalidade')
salvar_df(dim_tempo, 'dimtempo')
salvar_df(ft_vendas, 'fatovendas')