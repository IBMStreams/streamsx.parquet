
-----------------------------------------------------------------------------------------
CONNECT TO BIGSQL;
--CREATE SCHEMA ptest;
SET SCHEMA PTEST;
-----------------------------------------------------------------------------------------
DROP   TABLE IF EXISTS raw_events_sample ;
-----------------------------------------------------------------------------------------
CREATE EXTERNAL HADOOP TABLE raw_events_sample (
        source_port     INTEGER NOT NULL,
        tcp_length      INTEGER NOT NULL,
        mms             BIGINT NOT NULL,
        dest_port     INTEGER NOT NULL,
        file_length     BIGINT NOT NULL,
	f1 		REAL NOT NULL,
	f2 		DOUBLE NOT NULL
)
STORED AS PARQUETFILE
LOCATION '/user/bigsql/alex/parquet'
;

-----------------------------------------------------------------------------------------
MSCK REPAIR TABLE PTEST.raw_events_sample;
SELECT * FROM raw_events_sample;

quit;
