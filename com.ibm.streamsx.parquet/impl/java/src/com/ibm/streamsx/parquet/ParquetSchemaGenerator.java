package com.ibm.streamsx.parquet;

import org.apache.log4j.Logger;
   
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
   
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;

public class ParquetSchemaGenerator {
	
	private static ParquetSchemaGenerator instance = null;
	
	/**
	 * Logger 
	 */
	private final static Logger logger = Logger.getLogger(ParquetSchemaGenerator.class.getName());		

	
	public static ParquetSchemaGenerator getInstance() {
		if (instance == null) {
			instance = new ParquetSchemaGenerator();
		}
		
		return instance;
	}
	
	
	/**
     * Generates parquet schema from an input tuple 
     * @param context
     * @return
     * @throws Exception 
     */
    public String generateParquetSchema(OperatorContext context, List<String> attrsToSkip) throws Exception { 
    	Iterator<StreamingInput<Tuple>> streamingInputIt = context.getStreamingInputs().iterator();
    	StreamingInput<Tuple> currTuple = null;
    	StreamSchema currSchema = null;
    	StringBuffer parquetSchema = new StringBuffer();
    	int attrCount = 0;
    	java.util.List<Type> attrTypesList = new ArrayList<Type>();

    	Attribute attr;
    	Type attrType;
    	String attrName = null;
    	while (streamingInputIt.hasNext()) {
			currTuple = streamingInputIt.next();
			String inPortName = currTuple.getName();
			parquetSchema.append("message " + inPortName + "{ \n");    		
			currSchema = currTuple.getStreamSchema();   
			attrCount = currSchema.getAttributeCount();
			for (int i=0; i < attrCount;i++) {
				attr = currSchema.getAttribute(i);
				attrName = attr.getName();
				attrType = attr.getType();
				if (attrsToSkip != null && attrsToSkip.contains(attrName)) continue;
				if (attrType.getMetaType().isCollectionType()) {
					parquetSchema.append(SPLCollectionToParquetType(attr));
				} else {					
					parquetSchema.append("required " + SPLPrimitiveToParquetType(attrType) + " " + attrName + ";\n");   
					attrTypesList.add(attrType);
				}
			}
			parquetSchema.append("}\n");
		    logger.info("Generated parquet schema: \n'" + parquetSchema + "'");  		
    	}
    	
    	
    	return parquetSchema.toString();
    }
	
	public String SPLCollectionToParquetType(Attribute attr) throws Exception  {
		StringBuffer parquetSchema = new StringBuffer();
		Type.MetaType attrMetaType = attr.getType().getMetaType();	
		
		switch (attrMetaType) {
			// list and set are the same from Parquet schema perspective
			case LIST: 
			case SET:
			{
				parquetSchema.append("\trepeated " +  SPLPrimitiveToParquetType(attr.getType()) + " " + attr.getName());
				parquetSchema.append("} \n" );
				break;
			}
			case MAP: {
				parquetSchema.append("\trepeated group " + attr.getName() + "{ \n" );
				parquetSchema.append("\t\trequired " + SPLPrimitiveToParquetType(attr.getType()) + ";\n" );
				parquetSchema.append("\t\toptional " + SPLPrimitiveToParquetType(attr.getType()) + ";\n" );
				parquetSchema.append("} \n" );
				break;
			}
			default: {				
				throw new Exception("Parquet schema generation failure: unsupported type '" + attrMetaType + "'");
			}
		}
		
		return parquetSchema.toString();
	}
	
	public String SPLPrimitiveToParquetType(Type attrType) {
		Type.MetaType attrMetaType = attrType.getMetaType();
		
		switch (attrMetaType) {
			case BOOLEAN: {
				return "boolean";
			}
			case INT32: {
				return "int32";
			}
			case INT64: {
				return "int64";
			}
			case FLOAT32: {
				return "float";
			}
			case FLOAT64: {
				return "double";
			}
			case USTRING:
			case RSTRING: {
				return "binary";
			}
			case BLOB: {
				return "binary";
			}
			default: {			
				return "binary";			
			}
		}
	}

}
