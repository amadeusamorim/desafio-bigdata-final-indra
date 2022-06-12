# DESAFIO BIG DATA - TURMA INDRA UNIESP


## 📌 ESCOPO DO DESAFIO
* Neste desafio serão feitas as ingestões dos dados que estão na pasta dados/dados_entrada onde vamos ter alguns arquivos .csv de um banco de vendas.

       - VENDAS.CSV
       - CLIENTES.CSV
       - ENDERECO.CSV
       - REGIAO.CSV
       - DIVISAO.CSV

* Seu trabalho como engenheiro de dados é prover dados em uma pasta dados/dados_saida em .csv para ser consumido por um relatório em PowerBI que deverá ser construídi dentro da pasta 'app'.

### 📑 ETAPAS

* **Etapa 1** - Enviar os arquivos para o HDFS.
* **Etapa 2** - Criar tabelas no Hive.

              - TBL_VENDAS
              - TBL_VENDAS_STG
              - TBL_CLIENTES
              - TBL_CLIENTES_STG
              - TBL_ENDERECO
              - TBL_ENDERECO_STG
              - TBL_REGIAO
              - TBL_REGIAO_STG
              - TBL_DIVISAO
              - TBL_DIVISAO_STG
* **Etapa 3** - Processar os dados no Spark Efetuando suas devidas transformações.
* **Etapa 4** - Gravar as informações em tabelas.

              - FT_VENDAS
              - DIM_CLIENTES
              - DIM_TEMPO
              - DIM_LOCALIDADE
* **Etapa 5** - Exportar os dados para a pasta dados/dados_saida
* **Etapa 6** - Criar e editar o PowerBI com os dados que você trabalhou.
  * No PowerBI criar gráficos de vendas.
* **Etapa 7** - Criar uma documentação com os testes e etapas do projeto.

### REGRAS

* Campos strings vazios deverão ser preenchidos com 'Não informado'.
* Campos decimais ou inteiros nulos ou vazios, deversão ser preenchidos por 0.
* Atentem-se a modelagem de dados da tabela FATO e Dimensão.
* Na tabela FATO, pelo menos a métrica valor de vnda é um requisito obrigatório.
* Nas dimensões deverá conter valores únicos, não deverá conter valores repetidos.
   
### INSTRUÇÕES

* Executar a malha do Hive dentro do Container hive-server.
* Executar o processamento do Spark dentro do container spark.


## ✔️ ESTRUTURA E RESOLUTIVA

### :construction: Projeto em construção :construction:

Dentro do Container, aliadas as pastas já existentes, foram agregadas mais algumas pastas com scripts para execução do jobs da melhor forma.

Abaixo estão listadas as pastas e arquivos, representando a execução da Etapa 1, 2, 3, 4 e 5, com a descrição do que representa cada conteúdo:

* **app**: Pasta que constará o arquivo de execução do Power BI.
* **dados**
  * **dados_entrada**: Arquivos .csv que serão inputadas dentro da Tabela Externa Hive no HDFS.
  * **dados_saida**: Arquivos .csv com as tabelas dimensão e FATO, após tratamento via Spark, que serão utilizadas para a reprodução no Power BI.
* **hqls**: Pasta que conterá os arquivos .hql (scripts Hive) para execução. Cada tabela terá uma pasta diferente para fins de organização. Cada pasta das tabelas terão três arquivos, sendo: **create-external-table-stg** (responsável por criar a planilha externa do HDFS com seus respectivos metadados); **create-managed-table-wrk** (responsável por criar as tabelas internas também dentro do Hive, mas que poderá ser manipulada para seus devidos tratamentos); **insert-table-wrk** (script que levará os dados da tabela externa para a tabela interna, com o acréscimo da dt_foto, ou seja, a data da foto/informações).
  * **hql-clientes**
  * **hql-divisao**
  * **hql-endereco**
  * **hql-regiao**
  * **hql-vendas**
* **malha**: Pasta onde constará os scripts para ingestão das tabelas e execução do código em Spark
  * *jobs_hive*: Script para executar outros scripts relativos à criação da tabela externa e interna, com os seus devidos testes para confirmação se os dados realmente foram inseridos nos conformes.
  * *jobs_spark*: Script para executar o código criado em pyspark para tratamento dos dados da tabela interna e criação das dimensões e tabela FATO.
* **scripts**: Pasta que conterá todos os scripts em bash, para **criação de tabelas**, **inserção dos dados na tabela externa e interna**, nessa pasta também estará o arquivo .py que realizará o tratamento em Spark da tabela interna.
  * **create_tables**: Pasta que conterá os scripts para criação das 10 tabelas dentro do Hive (internas e externas), repassando as variáveis via Shell Script.
  * *insert_data_worked_table*: Script que passa variáveis e executa o .hql de inserção de dados na tabela interna derivado da tabela externa.
  * *update_data_external_table*: Script que passa varíáveis e informa o path do arquivo .csv para construção da tabela Externa. 
  * *job_processor*: Script python. Ele será responsável pelo tratamento dos dados da tabela interna.
* *rollback*: Script para deleção dos arquivos .csv do HDFS e das tabelas e BD do Hive, em caso de erro na execução das tabelas.
* *rollout*: Script para criação das pastas no HDFS e para chamar os scrips que criam as tabelas.

### ⚙️ Executando os testes

### 📊 Dashboard


### 🔧 Ferramentas utilizadas
- ``Shell Script``
- ``Hive / HQL``
- ``Spark``
- ``Power BI``

### ✒️ Autor do desafio

* **Caiuá França** - *desafio_bigdata_final* - [caiuafranca](https://github.com/caiuafranca/desafio_bigdata_final)



