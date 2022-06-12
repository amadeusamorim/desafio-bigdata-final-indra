CREATE DATABASE IF NOT EXISTS ${TARGET_DATABASE};

DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE};

CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE}(
	`Address Number` integer,
	`Business Family` string,
	`Business Unit` integer,
	`Customer` string,
	`CustomerKey` integer,
	`Customer Type` string,
    `Division` integer,
	`Line of Business` string,
	`Phone` string,
    `Region Code` integer,
	`Regional Sales Mgr` string,
	`Search Type` string
)
COMMENT 'Tabela Externa de Clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '${HDFS_DIR}'
tblproperties ('skip.header.line.count'='1', 'store.charset'='UTF-8', 'retrieve.charset'='UTF-8');