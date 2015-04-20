package com.ibm.streamsx.parquet;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;

import parquet.column.ParquetProperties.WriterVersion;
import parquet.hive.write.DataWritableWriteSupport;
import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.metadata.CompressionCodecName;

import parquet.hadoop.ParquetWriter;

public class ParquetWriterFactory {

	private static ParquetWriterFactory instance = null;

	public static ParquetWriterFactory getInstance() {
		if (instance == null) {
			instance = new ParquetWriterFactory();
		}
		return instance;
	}

	public ParquetWriter<ArrayWritable> createArrayWritableWriter(Path outFilePath, 
																  final String schema,
																  CompressionCodecName compressionType,
																  int blockSize,
																  int pageSize,
																  int dictPageSize,
																  boolean enableDictionary,
																  boolean enableSchemaValidation,
																  WriterVersion parquetWriterVersion) throws IOException {

		ParquetWriter<ArrayWritable> res = new ParquetWriter<ArrayWritable>(
				outFilePath, new DataWritableWriteSupport() {
					@Override
					public WriteContext init(Configuration configuration) {
						if (configuration
								.get(DataWritableWriteSupport.PARQUET_HIVE_SCHEMA) == null) {
							String sche = schema.toString();
							configuration
									.set(DataWritableWriteSupport.PARQUET_HIVE_SCHEMA,
											sche);
						}
						return super.init(configuration);
					}
				},
				// compression typewriter
				compressionType,
				// The block size is the size of a row group being buffered in
				// memory.
				// Tthis limits the memory usage when writing.
				// Larger values will improve the IO.
				blockSize,
				// The page size is for compression.
				// When reading, each page can be decompressed independently.
				// A block is composed of pages. The page is the smallest unit
				// that must be read fully to access a single record.
				// If this value is too small, the compression will deteriorate
				pageSize,
				// There is one dictionary page per column per row
				// group when dictionary encoding is used.
				// The dictionary page size works like the page size but for
				// dictionary
				dictPageSize,
				// control dictionary encoding
				enableDictionary,
				// control schema validation
				enableSchemaValidation,
				// writer version
				parquetWriterVersion);

		return res;
	}

}
