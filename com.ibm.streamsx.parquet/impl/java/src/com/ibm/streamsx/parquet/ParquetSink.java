/* Generated by Streams Studio: July 20, 2014 9:47:24 PM GMT+03:00 */
package com.ibm.streamsx.parquet;

import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.meta.CollectionType;
import com.ibm.streams.operator.meta.MapType;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.metrics.Metric.Kind;
import com.ibm.streams.operator.model.CustomMetric;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.Timestamp;

import parquet.column.ParquetProperties.WriterVersion;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hive.writable.BinaryWritable;
import parquet.hive.write.DataWritableWriteSupport;
import parquet.io.api.Binary;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

/**
 * Operator allows to write data in Parquet format from streaming applications.
 * @author apyasic
 *
 */
@PrimitiveOperator(name = "ParquetSink", namespace = "com.ibm.streamsx.parquet", description = "Java Operator ParquetSink")
@InputPorts({
		@InputPortSet(description = "Port for tuples ingestion", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious),
		@InputPortSet(description = "Optional input ports", optional = true, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious) })
@OutputPorts({
		@OutputPortSet(description = "Port that produces tuples containing names of the closed files", cardinality = 1, optional = true, windowPunctuationOutputMode = WindowPunctuationOutputMode.Free) })
@Libraries({ "impl/lib/*", "lib/*" })		
@SharedLoader(value = true)
public class ParquetSink extends AbstractOperator {

	/*
	 * Operator parameters
	 */
	private String hdfsUri = null;
	private String hdfsUser = null;
	private String rootPath = null;
	private int blockSize = DEFAULT_BLOCK_SIZE;
	private int pageSize = DEFAULT_PAGE_SIZE;
	private int dictPageSize = ParquetConstants.DEFAULT_DICTIONARY_PAGE_SIZE;
	private boolean closeOnPunct = false;
	private CompressionCodecName compressionType = CompressionCodecName.UNCOMPRESSED;
	private String compressionTypeStr = CompressionCodecName.UNCOMPRESSED.toString();
	private boolean enableDictionaryEncoding = false;
	private boolean enableSchemaValidation = false;
	private String file = null;
	private boolean overwrite = false;
	private boolean autoCreate = true;
	private WriterVersion writerVersion = WriterVersion.PARQUET_1_0;
	private int tuplesPerFile = ParquetConstants.DEFAULT_TUPLES_PER_FILE;
	// partition keys list
	private List<String> partitionKeyNames;
	// private List<Attribute> partitionKeyAttrs;
	private List<String> partitionValueAttrNames;
	private boolean skipPartitionAttrs = true;

	/*
	 * Metrics
	 */
	private Metric avgFileWriteTime;
	private Metric maxFileWriteTime;
	private Metric minFileWriteTime;
	// closed files number
	private Metric nClosedFiles;
	// opened files number
	private Metric nOpenedFiles;
	// total file processing time
	private long totalFileProcTime = 0;
	// file processing start time stamp
	private long fileProcStartTimestamp = 0;

	/*
	 * Auxiliary members
	 */
	private MessageType parquetSchema = null;
	// private ParquetWriter writer = null;
	private HashMap<String, ParquetWriterMapValue> writerMap = new HashMap<String, ParquetWriterMapValue>();
	private Configuration config = null;
	private Writable[] writableValues = null;
	private ArrayWritable arrayWritable = new ArrayWritable(Writable.class, null);
	// private int currTuplesInFile = 0;
	private FileSystem fs = null;
	private Path staticOutPath = null;

	// auxiliry class for atribute metadata extraction
	private AttrMetadataExtractor attrMetadataExtractor;

	/**
	 * Logger
	 */
	private final static Logger logger = Logger.getLogger(ParquetSink.class.getName());

	/**
	 * Initialize this operator. Called once before any tuples are processed.
	 * 
	 * @param context
	 *            OperatorContext for this operator.
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {

		// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);

		if (logger.isTraceEnabled()) {
			logger.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId()
					+ " in Job: " + context.getPE().getJobId());
		}

		// initialize auxiliary class for input
		// tuple attributes metadata extraction
		attrMetadataExtractor = new AttrMetadataExtractor(context);

		// initialize metrics
		avgFileWriteTime.setValue(0);
		maxFileWriteTime.setValue(0);
		minFileWriteTime.setValue(0);
		nOpenedFiles.setValue(0);
		nClosedFiles.setValue(0);

		// HDFS client configuration
		config = new Configuration();

		// This property is used only if the value of
		// dfs.client.block.write.replace-datanode-on-failure.enable is true.
		// ALWAYS: always add a new datanode when an existing datanode is
		// removed.
		// NEVER: never add a new datanode.
		// DEFAULT: Let r be the replication number. Let n be the number of
		// existing dat..array_element:  anodes.
		// Add a new datanode only if r is greater than or equal to 3 and either
		// (1) floor(r/2) is
		// greater than or equal to n; or (2) r is greater than n and the block
		// is hflushed/appended.
		config.set("dfs.client.block.write.replace-datanode-on-failure.policy", "ALWAYS");

		// When using DEFAULT or ALWAYS, if only one DataNode succeeds in the
		// pipeline,
		// the recovery will never succeed and client will not be able to
		// perform the write.
		// This problem is addressed with this configuration
		// property:dfs.client.block.write.replace-datanode-on-failure.best-effort
		// which defaults to false. With the default setting, the client will
		// keep trying until
		// the specified policy is satisfied. When this property is set to true,
		// even if the specified
		// policy can't be satisfied (for example, there is only one DataNode
		// that succeeds in the pipeline,
		// which is less than the policy requirement), the client will still be
		// allowed to continue to write.
		config.set("dfs.client.block.write.replace-datanode-on-failure.best-effort", "true");

		// config.set("fs.defaultFS", getHdfsUri());
		fs = FileSystem.get(new URI(getHdfsUri()), config, getHdfsUser());

		// set username
		System.setProperty("HADOOP_USER_NAME", getHdfsUser());

		// fs = FileSystem.get(config);

		compressionType = CompressionCodecName.valueOf(compressionTypeStr);
		// generate schema from an output tuple format
		String parquetSchemaStr = ParquetSchemaGenerator.getInstance().generateParquetSchema(context,
				partitionValueAttrNames);
		parquetSchema = MessageTypeParser.parseMessageType(parquetSchemaStr);

		int writablesNum = skipPartitionAttrs && partitionKeyNames != null
				? attrMetadataExtractor.getAttrCount() - partitionKeyNames.size()
				: attrMetadataExtractor.getAttrCount();
		if (logger.isTraceEnabled()) {
			logger.trace("Number of attributes to write is " + writablesNum + " out of "
					+ attrMetadataExtractor.getAttrCount());
		}
		writableValues = new Writable[writablesNum];

		DataWritableWriteSupport.setSchema(parquetSchema, config);

		// the file might be opened here if the path
		// generation logic is not tuple specific
		staticOutPath = new Path(hdfsUri + ParquetConstants.PATH_DELIMITER + rootPath);
		if (!partitionSupportRequired() && getAutoCreate()) {

			Path outFilePath = generatePath(null, getFile(), 0);
			String tempFileName = "." + getFile();
			Path tempOutFilePath = generatePath(null, tempFileName, 0);

			if (logger.isTraceEnabled()) {
				logger.trace("Partition support not required. Using a static output folder '" + outFilePath + "'");
			}

			// remove output file is necessary
			if (getOverwrite()) {
				if (logger.isTraceEnabled()) {
					logger.trace("About to recursively  remove files from '" + outFilePath + "'");
				}

				// remove temporary files if exists
				recursivelyRemoveFile(tempOutFilePath);
				
				// remove original files if exists
				recursivelyRemoveFile(outFilePath);

			}

			ParquetWriter<ArrayWritable> writer = openFile(tempOutFilePath);
			writerMap.put("", new ParquetWriterMapValue(writer));

		} else {
			if (logger.isTraceEnabled()) {
				logger.trace(
						"Partition support required. Using a dynamic (tuple data based) output folder generation mechanism.");
			}
		}

	}

	private Path generatePath(String tupleOutFileFolder, String file, int fileIndex) {

		return new Path(
				staticOutPath + (tupleOutFileFolder != null ? ParquetConstants.PATH_DELIMITER + tupleOutFileFolder : "")
						+ ParquetConstants.PATH_DELIMITER + file.replaceAll("\\{ID\\}", String.valueOf(fileIndex)));
	}

	/**
	 * Check params and ports at compile time
	 * 
	 * @param checker
	 * @throws Exception
	 */
	@ContextCheck(compile = true)
	public static void checkParamsAndPortsAtCompile(OperatorContextChecker checker) throws Exception {

		/*
		 * Check parameter co-existance constraints
		 */
		checker.checkExcludedParameters(ParquetConstants.CLOSE_ON_PUNCT_PNAME, ParquetConstants.TUPLES_PER_FILE_PNAME);
		checker.checkDependentParameters(ParquetConstants.PARTITION_VALUE_ATTR_NAMES_PNAME,
				ParquetConstants.PARTITION_KEY_NAMES_PNAME);

		/*
		 * Check input/output port number constraints
		 */
		OperatorContext context = checker.getOperatorContext();
		if (context.getNumberOfStreamingInputs() != 1) {
			checker.setInvalidContext("The operator should have a single input port.", null);
		}
		if (context.getNumberOfStreamingOutputs() > 1) {
			checker.setInvalidContext("The operator should have a single optional output port.", null);
		}

	}

	/**
	 * Check param values at runtime
	 * 
	 * @param checker
	 * @throws Exception
	 */
	@ContextCheck(compile = false, runtime = true)
	public static void checkParamsAndPortsAtRuntime(OperatorContextChecker checker) throws Exception {
		OperatorContext context = checker.getOperatorContext();
		// "compile" time instance vs the one defined in initialize
		AttrMetadataExtractor attrMetadataExtractor = new AttrMetadataExtractor(context);
		// list of an input port attribute names
		List<String> portAttrNames = attrMetadataExtractor.getAttrNamesList(0);
		// list of attribute names as specified in parameter value
		List<String> partitionValueAttrNames = context
				.getParameterValues(ParquetConstants.PARTITION_VALUE_ATTR_NAMES_PNAME);
		List<String> partitionKeyNameNames = context.getParameterValues(ParquetConstants.PARTITION_KEY_NAMES_PNAME);
		if (partitionValueAttrNames.size() != partitionKeyNameNames.size()) {
			checker.setInvalidContext("The number of keys specified in '" + ParquetConstants.PARTITION_KEY_NAMES_PNAME
					+ "' operator param should match the number of values specified in '"
					+ ParquetConstants.PARTITION_VALUE_ATTR_NAMES_PNAME + "' operator param", null);
		}

		for (String partitionValueAttrName : partitionValueAttrNames) {
			if (!portAttrNames.contains(partitionValueAttrName)) {
				String errMsg = "Illegal operator configuration: the attribute '" + partitionValueAttrName
						+ "' specified in  'partitionValueAttrNames' operator parameter is not defined in the input port schema";
				checker.setInvalidContext(errMsg, null);
			}
		}
	}

	/**
	 * Notification that initialization is complete and all input and output
	 * ports are connected and ready to receive and submit tuples.
	 * 
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void allPortsReady() throws Exception {
		// This method is commonly used by source operators.
		// Operators that process incoming tuples generally do not need this
		// notification.
		OperatorContext context = getOperatorContext();
		if (logger.isTraceEnabled()) {
			logger.trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId()
					+ " in Job: " + context.getPE().getJobId());
		}
	}

	/**
	 * Process an incoming tuple that arrived on the specified port.
	 * 
	 * @param stream
	 *            Port the tuple is arriving on.
	 * @param tuple
	 *            Object representing the incoming tuple.
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {
		// by default generate un-partitioned file path
		String tupleOutFileFolder = null;
		if (partitionSupportRequired()) {
			// calculate the tuple out path. The
			// path is derived from the tuple data.
			tupleOutFileFolder = genOutFileFolder(tuple);
		} else {
			tupleOutFileFolder = "";
		}

		// parquet writer for this tuple already exists - no need to create a
		// new one
		if (!writerMap.containsKey(tupleOutFileFolder)) {
			// prepare out folder if necessary
			if (!pathExists(new Path(staticOutPath + ParquetConstants.PATH_DELIMITER + tupleOutFileFolder))) {
				if (logger.isTraceEnabled()) {
					logger.trace("Folder '" + staticOutPath + ParquetConstants.PATH_DELIMITER + tupleOutFileFolder
							+ "' doesn't exist. Creating.");
				}
				createFolder(new Path(staticOutPath + ParquetConstants.PATH_DELIMITER + tupleOutFileFolder),
						ParquetConstants.DEFAULT_FOLDER_PERMISSIONS);
			}
			writerMap.put(tupleOutFileFolder, new ParquetWriterMapValue(null));
		}

		ParquetWriterMapValue pwv = (ParquetWriterMapValue) writerMap.get(tupleOutFileFolder);

		if (pwv.pw == null) {
			Path outFilePath = generatePath(tupleOutFileFolder, getFile(), pwv.writerFileIndex);
			String tempFileName = "." + getFile();
			Path tempOutFilePath = generatePath(tupleOutFileFolder, tempFileName, pwv.writerFileIndex);

			if (logger.isTraceEnabled()) {
				logger.trace("Tuple based partition path '" + outFilePath + "'");
			}

			// remove old file if necessary
			if (getOverwrite()) {
				// remove temporary files if exists
				if (pathExists(tempOutFilePath)) recursivelyRemoveFile(tempOutFilePath);
				
				// remove original files if exists
				if (pathExists(outFilePath)) recursivelyRemoveFile(outFilePath);
			}
			
			if (logger.isTraceEnabled()) {
				logger.trace("About to open file '" + outFilePath + "' for tuple '" + tuple + "'");
			}
			ParquetWriter<ArrayWritable> writer = openFile(tempOutFilePath);
			pwv.pw = writer;
		}

		List<Type> attrTypeList = attrMetadataExtractor.getAttrTypesList(stream);
		List<String> attrNamesList = attrMetadataExtractor.getAttrNamesList(stream);

		for (int i = 0; i < attrMetadataExtractor.getAttrCount(); i++) {
			Type attrType = attrTypeList.get(i);
			if (partitionSupportRequired() && skipPartitionAttrs) {
				String attrName = (String) attrNamesList.get(i);
				if (partitionValueAttrNames != null && partitionValueAttrNames.contains(attrName)) {
					continue;
				}
			}
			switch (attrType.getMetaType()) {
			case LIST:
			case SET:
				Collection<?> collection = (attrType.getMetaType() == Type.MetaType.LIST ? tuple.getList(i)
						: tuple.getSet(i));
				if (collection.size() > 0) {
					final Writable[] container = new Writable[1];
					final Writable[] values = new Writable[collection.size()];
					final Type.MetaType elementMetaType = ((CollectionType) attrType).getElementType().getMetaType();
					int j = 0;
					Iterator<?> iter = collection.iterator();
					while (iter.hasNext()) {
						values[j++] = SPLPrimitiveWritableObj(elementMetaType, iter.next());
					}
					container[0] = new ArrayWritable(Writable.class, values);
					final ArrayWritable collectionWritable = new ArrayWritable(Writable.class, container);
					writableValues[i] = collectionWritable;
				} else {
					writableValues[i] = null;
				}
				break;
			case MAP:
				Map<?, ?> map = tuple.getMap(i);
				if (map.size() > 0) {
					final Writable[] mapContainer = new Writable[1];
					final Writable[] mapValues = new Writable[map.size()];
					Iterator<?> mapIterator = map.entrySet().iterator();
					int j = 0;
					while (mapIterator.hasNext()) {
						Map.Entry entry = (Map.Entry) mapIterator.next();
						final Writable[] pair = new Writable[2];
						pair[0] = SPLPrimitiveWritableObj(((MapType) attrType).getKeyType().getMetaType(),
								entry.getKey());
						pair[1] = SPLPrimitiveWritableObj(((MapType) attrType).getValueType().getMetaType(),
								entry.getValue());
						mapValues[j++] = new ArrayWritable(Writable.class, pair);
					}
					mapContainer[0] = new ArrayWritable(Writable.class, mapValues);
					final ArrayWritable mapWritable = new ArrayWritable(Writable.class, mapContainer);
					writableValues[i] = mapWritable;
				} else {
					writableValues[i] = null;
				}
				break;
			default:
				Object attrValue = tuple.getObject(i);
				writableValues[i] = SPLPrimitiveWritableObj(attrType.getMetaType(), attrValue);
				break;
			}
		}

		arrayWritable.set(writableValues);
		pwv.pw.write(arrayWritable);
		pwv.writerTupleCount++;
		// close the current file and open a new one
		// if tuplesPerFile parameter defined.
		if (getTuplesPerFile() > 0 && (pwv.writerTupleCount == tuplesPerFile)) {
			closeFile(tupleOutFileFolder);
		}
	}

	/**
	 * Process an incoming punctuation that arrived on the specified port.
	 * 
	 * @param stream
	 *            Port the punctuation is arriving on.
	 * @param mark
	 *            The punctuation mark
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public void processPunctuation(StreamingInput<Tuple> stream, Punctuation mark) throws Exception {
		super.processPunctuation(stream, mark);
		if (getCloseOnPunct()) {
			for (String tupleOutFileFolder : writerMap.keySet()) {
				closeFile(tupleOutFileFolder);
			}
		}
	}

	/**
	 * Shutdown this operator.
	 * 
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void shutdown() throws Exception {
		OperatorContext context = getOperatorContext();
		if (logger.isTraceEnabled()) {
			logger.trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId()
					+ " in Job: " + context.getPE().getJobId());
		}

		for (String tupleOutFileFolder : writerMap.keySet()) {
			closeFile(tupleOutFileFolder);
		}

		writerMap.clear();

		// Must call super.shutdown()
		super.shutdown();
	}

	/*
	 * Parameter definitions
	 */

	@Parameter(name = "blockSize", description = "Block Size", optional = true)
	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public int getBlockSize() {
		return this.blockSize;
	}

	@Parameter(name = "pageSize", description = "Page Size", optional = true)
	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getPageSize() {
		return this.pageSize;
	}

	@Parameter(name = "hdfsUri", description = "Target HDFS URI", optional = false)
	public void setHdfsUri(String hdfsUri) {
		this.hdfsUri = hdfsUri;
	}

	public String getHdfsUri() {
		return this.hdfsUri;
	}

	@Parameter(name = "hdfsUser", description = "HDFS User", optional = false)
	public void setUser(String hdfsUser) {
		this.hdfsUser = hdfsUser;
	}

	public String getHdfsUser() {
		return this.hdfsUser;
	}

	@Parameter(name = "rootPath", description = "Target directory root path", optional = false)
	public void setRootPath(String rootPath) {
		this.rootPath = rootPath;
	}

	public String getRootPath() {
		return this.rootPath;
	}

	@Parameter(name = "file", description = "Target file name", optional = false)
	public void setFile(String file) {
		this.file = file;
	}

	public String getFile() {
		return this.file;
	}

	@Parameter(name = "dictPageSize", description = "Dictionary page size", optional = true)
	public void setDictPageSize(int dictPageSize) {
		this.dictPageSize = dictPageSize;
	}

	public int getDictPageSize() {
		return this.dictPageSize;
	}

	@Parameter(name = "closeOnPunct", description = "Close target file on punctuation received", optional = true)
	public void setCloseOnPunct(boolean closeOnPunct) {
		this.closeOnPunct = closeOnPunct;
	}

	public boolean getCloseOnPunct() {
		return this.closeOnPunct;
	}

	@Parameter(name = "compressionType", description = "Target file compression. Supported values are: 'snappy', 'gzip' and 'uncomressed' (default).", optional = true)
	public void setCompressionType(String compressionTypeStr) {
		this.compressionTypeStr = compressionTypeStr.toUpperCase();
	}

	public String getCompressionType() {
		return this.compressionTypeStr;
	}

	@Parameter(name = "enableDictionaryEncoding", description = "Enable dictionary encoding", optional = true)
	public void setEnableDictionaryEncoding(boolean enableDictionaryEncoding) {
		this.enableDictionaryEncoding = enableDictionaryEncoding;
	}

	public boolean getEnableDictionaryEncoding() {
		return enableDictionaryEncoding;
	}

	@Parameter(name = "enableSchemaValidation", description = "Enable parquet schema validation", optional = true)
	public void setEnableSchemaValidation(boolean enableSchemaValidation) {
		this.enableSchemaValidation = enableSchemaValidation;
	}

	public boolean getEnableSchemaValidation() {
		return enableSchemaValidation;
	}

	@Parameter(name = "overwrite", description = "Overwrite target file is already exists", optional = true)
	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}

	public boolean getOverwrite() {
		return this.overwrite;
	}

	@Parameter(name = "autoCreate", description = "Automatically create output file on application start-up", optional = true)
	public void setAutoCreate(boolean autoCreate) {
		this.autoCreate = autoCreate;
	}

	public boolean getAutoCreate() {
		return this.autoCreate;
	}

	@Parameter(name = "writerVersion", description = "Parquet writer version. Supported values are 1.0 and 2.0", optional = true)
	public void setWriterVersion(String writerVersion) {
		this.writerVersion = WriterVersion.valueOf(writerVersion);
	}

	public WriterVersion getWriterVersion() {
		return this.writerVersion;
	}

	@Parameter(name = "tuplesPerFile", description = "Close target file after number of tuples specified in this param was written.", optional = true)
	public void setTuplesPerFile(int tuplesPerFile) {
		this.tuplesPerFile = tuplesPerFile;
	}

	public int getTuplesPerFile() {
		return this.tuplesPerFile;
	}

	@Parameter(name = "partitionKeyNames", description = "Partition key names. The keys used in combination with values specified by 'partitionValueAttrNames' for target files partitioning.", optional = true, cardinality = -1)
	public void setPartitionKeyNames(List<String> partitionKeyNames) {
		if (logger.isTraceEnabled()) {
			logger.trace("Set partition key names to '" + partitionKeyNames + "'");
		}
		this.partitionKeyNames = partitionKeyNames;
	}

	public List<String> getPartitionKeyNames() {
		return this.partitionKeyNames;
	}

	@Parameter(name = "partitionValueAttrNames", description = "Attribute names to use for partitioning. Usually the attributes contains timestamp information like year/month/day", optional = true, cardinality = -1)
	public void setPartitionValueAttrNames(List<String> attrNames) {
		partitionValueAttrNames = attrNames;
	}

	public List<String> getPartitionValueAttrNames() {
		return this.partitionValueAttrNames;
	}

	@Parameter(name = "skipPartitionAttrs", description = "Specifies if partition attributes should be wriiten to the target file vs having them as a part of the directory structure only.", optional = true)
	public void setSkipPartitionAttrs(boolean skipPartitionAttrs) {
		this.skipPartitionAttrs = skipPartitionAttrs;
	}

	public boolean getSkipPartitionAttrs() {
		return this.skipPartitionAttrs;
	}

	/*
	 * Metric definitions
	 */

	@CustomMetric(description = "Average file write time", kind = Kind.COUNTER)
	public void setavgFileWriteTime(Metric avgFileWriteTime) {
		this.avgFileWriteTime = avgFileWriteTime;
	}

	public Metric getavgFileWriteTime() {
		return avgFileWriteTime;
	}

	@CustomMetric(description = "Max file write time", kind = Kind.COUNTER)
	public void setmaxFileWriteTime(Metric maxFileWriteTime) {
		this.maxFileWriteTime = maxFileWriteTime;
	}

	public Metric getmaxFileWriteTime() {
		return maxFileWriteTime;
	}

	@CustomMetric(description = "Min file write time", kind = Kind.COUNTER)
	public void setminFileWriteTime(Metric minFileWriteTime) {
		this.minFileWriteTime = minFileWriteTime;
	}

	public Metric getminFileWriteTime() {
		return minFileWriteTime;
	}

	@CustomMetric(description = "Closed files num", kind = Kind.COUNTER)
	public void setnClosedFiles(Metric nClosedFiles) {
		this.nClosedFiles = nClosedFiles;
	}

	public Metric getnClosedFiles() {
		return nClosedFiles;
	}

	@CustomMetric(description = "Opened files num", kind = Kind.COUNTER)
	public void setnOpenedFiles(Metric nOpenedFiles) {
		this.nOpenedFiles = nOpenedFiles;
	}

	public Metric getnOpenedFiles() {
		return nOpenedFiles;
	}

	/*
	 * Auxiliary methods
	 */

	private Writable SPLPrimitiveWritableObj(Type.MetaType metaType, Object value) {
		switch (metaType) {
		case BOOLEAN: {
			Boolean cValue = (Boolean) value;
			return new BooleanWritable(cValue.booleanValue());
		}
		case UINT8:
		case INT8: {
			Byte cValue = ((Byte) value).byteValue();
			return new ByteWritable(cValue.byteValue());
		}
		case UINT16:
		case INT16: {
			Short cValue = ((Short) value).shortValue();
			return new ShortWritable(cValue.shortValue());
		}
		case UINT32:
		case INT32: {
			Integer cValue = (Integer) value;
			return new IntWritable(cValue.intValue());
		}
		case UINT64:
		case INT64: {
			Long cValue = (Long) value;
			return new LongWritable(cValue.longValue());
		}
		case FLOAT32: {
			Float cValue = (Float) value;
			return new FloatWritable(cValue.floatValue());
		}
		case FLOAT64: {
			Double cValue = (Double) value;
			return new DoubleWritable(cValue.doubleValue());
		}
		case RSTRING: {
			RString cValue = (RString) value;
			return new BinaryWritable(Binary.fromByteArray(cValue.getData()));
		}
		case TIMESTAMP: {
			Timestamp cValue = (Timestamp) value;
			return new BinaryWritable(
					NanoTimeUtils.getNanoTime((cValue.getSeconds() >= 0 && cValue.getNanoseconds() >= 0
							? cValue.getSQLTimestamp() : new java.sql.Timestamp(0))).toBinary());
		}
		case BLOB: {
			Blob cValue = (Blob) value;
			return new BinaryWritable(Binary.fromByteArray(cValue.getByteBuffer().array()));
		}
		default: {
			Blob cValue = (Blob) value;
			return new BinaryWritable(Binary.fromByteArray(cValue.getByteBuffer().array()));
		}
		}
	}

	/**
	 * Recursively create folder in HDFS if necessary
	 */
	private void createFolder(Path outFilePath, FsPermission fsPermission) throws IOException {
		try {
			if (!fs.exists(outFilePath)) {
				FileSystem.mkdirs(fs, outFilePath, fsPermission);
			}
		} catch (IOException ioe) {
			logger.error("Failed to remove file '" + outFilePath.toUri().toString() + "'\n");
			throw ioe;
		}
	}

	private boolean pathExists(Path tupleOutFileFolder) throws IOException {
		return fs.exists(tupleOutFileFolder);
	}

	/**
	 * Recursively remove file in HDFS
	 */
	private void recursivelyRemoveFile(Path outFilePath) throws IOException {
		try {
			if (fs.exists(outFilePath)) {
				fs.delete(outFilePath, true);
			}
		} catch (IOException ioe) {
			logger.error("Failed to remove file '" + outFilePath.toUri().toString() + "'\n");
			throw ioe;
		}
	}

	private ParquetWriter<ArrayWritable> openFile(Path outFilePath) throws IOException {
		if (logger.isTraceEnabled()) {
			logger.trace("About to initialize parquet write for file '" + outFilePath.toString() + "' with: "
					+ "compression type '" + getCompressionType() + "'," + "block size '" + getBlockSize() + "',"
					+ "page size '" + getPageSize() + "'," + "dictionary page size '" + getDictPageSize() + "'");
		}

		// update metric state variables
		fileProcStartTimestamp = System.currentTimeMillis();
		nOpenedFiles.incrementValue(1l);

		return ParquetWriterFactory.getInstance().createArrayWritableWriter(outFilePath, this.parquetSchema.toString(),
				this.compressionType, getBlockSize(), getPageSize(), getDictPageSize(), getEnableDictionaryEncoding(),
				getEnableSchemaValidation(), WriterVersion.PARQUET_1_0);

	}

	private void closeFile(String tupleOutFileFolder) throws Exception, IOException {
		ParquetWriterMapValue pwv = (ParquetWriterMapValue) writerMap.get(tupleOutFileFolder);
		if (pwv.pw != null) {
			pwv.pw.close();
			// writerMap.remove(outFilePath);
			fs.rename(generatePath(tupleOutFileFolder, "." + file, pwv.writerFileIndex),
					generatePath(tupleOutFileFolder, file, pwv.writerFileIndex));

			OperatorContext context = getOperatorContext();

			if (context.getNumberOfStreamingOutputs() > 0) {
				StreamingOutput<OutputTuple> outStream = getOutput(0);
				OutputTuple outTuple = outStream.newTuple();

				int fileAttributeIndex;
				if ((fileAttributeIndex = outStream.getStreamSchema().getAttributeIndex("file")) >= 0) {
					outTuple.setString(fileAttributeIndex,
							generatePath(tupleOutFileFolder, file, pwv.writerFileIndex).toString());
				}

				int partitionAttributeIndex;
				if ((partitionAttributeIndex = outStream.getStreamSchema().getAttributeIndex("partition")) >= 0) {
					outTuple.setString(partitionAttributeIndex, tupleOutFileFolder);
				}

				outStream.submit(outTuple);
			}
		}

		pwv.writerTupleCount = 0;
		pwv.pw = null;
		// update metric
		pwv.writerFileIndex++;

		nClosedFiles.incrementValue(1l);
		long currFileProcValue = System.currentTimeMillis() - fileProcStartTimestamp;
		totalFileProcTime += currFileProcValue;
		avgFileWriteTime.setValue(totalFileProcTime / nClosedFiles.getValue());

		if (maxFileWriteTime.getValue() < currFileProcValue) {
			maxFileWriteTime.setValue(currFileProcValue);
		}

		if ((minFileWriteTime.getValue() > currFileProcValue) || (minFileWriteTime.getValue() == 0)) {
			minFileWriteTime.setValue(currFileProcValue);
		}
	}

	/**
	 * @return - true if partition support is required as defined by operator
	 *         configuration.
	 */
	private boolean partitionSupportRequired() {
		return partitionKeyNames != null && partitionKeyNames.size() > 0;
	}

	private String genOutFileFolder(Tuple tuple) {
		StringBuffer resBuf = new StringBuffer();
		for (int i = 0; i < partitionValueAttrNames.size(); i++) {
			if (i > 0) {
				resBuf.append(ParquetConstants.PATH_DELIMITER);
			}
			resBuf.append((String) partitionKeyNames.get(i) + ParquetConstants.PARTITION_NVP_DELIMITER
					+ tuple.getString((String) partitionValueAttrNames.get(i)));
		}

		return resBuf.toString();

	}

	class ParquetWriterMapValue {
		ParquetWriter<ArrayWritable> pw;
		int writerTupleCount = 0;
		int writerFileIndex = 0;

		ParquetWriterMapValue(ParquetWriter<ArrayWritable> pw) {
			this.pw = pw;
		}
	}

}
