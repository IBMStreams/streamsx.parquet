package com.ibm.streamsx.parquet;

import org.apache.hadoop.fs.permission.FsPermission;

import parquet.hadoop.metadata.CompressionCodecName;

public class ParquetConstants {
	// default dictionary page size
    public static int DEFAULT_DICTIONARY_PAGE_SIZE = 268435456; //256*1024*1024

    public static CompressionCodecName DEFAULT_COMPRESSION_CODEC_NAME = CompressionCodecName.UNCOMPRESSED;
    public static int DEFAULT_TUPLES_PER_FILE = -1;

    /**
     * Parameter names
     */
    public static String CLOSE_ON_PUNCT_PNAME = "closeOnPunct";
    public static final String TUPLES_PER_FILE_PNAME = "tuplesPerFile";
    public static final String PARTITION_VALUE_ATTR_NAMES_PNAME = "partitionValueAttrNames";
    public static final String PARTITION_KEY_NAMES_PNAME = "partitionKeyNames";    

    /**
     * Auxiliary constants
     */
    public static String PATH_DELIMITER = "/";
    public static String PARTITION_NVP_DELIMITER = "=";
    public static FsPermission DEFAULT_FOLDER_PERMISSIONS = FsPermission.createImmutable((short)0755);

}
