package com.ibm.streamsx.parquet.test;

public class ParquetTestConstants {
	
	/**
	 * Sample schema of parquet sink for a test purposes.
	 * Should be aligned with appropriate BigSQL table
	 * structure:
	 * CREATE EXTERNAL HADOOP TABLE raw_events_alex (
        ts              VARCHAR (25) NOT NULL, 
        computer_id     VARCHAR (20) NOT NULL, 
        process_name    VARCHAR (25) NOT NULL, 
        operation       VARCHAR (20) NOT NULL, 
        oper_result     VARCHAR (20) NOT NULL, 
        source_host     VARCHAR (15) NOT NULL, 
        source_port     INTEGER NOT NULL, 
        tcp_length      INTEGER NOT NULL, 
        mms             BIGINT NOT NULL, 
        dest_host       VARCHAR (15) NOT NULL, 
        dest_port       INTEGER NOT NULL, 
        filename        VARCHAR (30) NOT NULL, 
        file_length     BIGINT NOT NULL,
       PRIMARY KEY ( ts, computer_id, process_name, source_host, dest_host)
	   PARTITIONED BY (
        	year    SMALLINT NOT NULL, 
        	month   TINYINT NOT NULL, 
        	day     TINYINT NOT NULL, 
        	hour    TINYINT NOT NULL
		)
		STORED AS PARQUETFILE
		LOCATION '/Data/Bigsql/jupiter2/batch_netaction'
		;
	 */
	public static String SAMPLE_PARQUET_SINK_IN_SCHEMA = 
			"tuple<rstring TS," 
            + "rstring COMPUTER_ID,"
            + "rstring PROCESS_NAME,"
            + "rstring OPERATION,"                         
            + "rstring OPER_RESULT," 
            + "rstring SOURCE_HOST," 
            + "int32 SOURCE_PORT," 
            + "int32 TCP_LENGTH," 
            + "int64 MMS," 
            + "rstring DEST_HOST," 
            + "int32 DEST_PORT," 
            + "rstring FILENAME," 
            + "int64 FILE_LENGTH," 
            + "int32 YEAR," 
            + "int32 MONTH," 
            + "int32 DAY," 
            + "int32 HOUR>";
	        //+ "rstring yearAttr," 
	        //+ "rstring monthAttr," 
	        //+ "rstring dayAttr," 
	        //+ "rstring hourAttr>";
	
}
