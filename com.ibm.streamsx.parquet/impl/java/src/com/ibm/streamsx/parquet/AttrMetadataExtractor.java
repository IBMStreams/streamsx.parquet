package com.ibm.streamsx.parquet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.OperatorContext;

/**
 * Aixiliary class for attributes metadata extraction
 * @author apyasic
 *
 */
public class AttrMetadataExtractor {

	private OperatorContext context;
	private int attrCount = 0;	
	private Map<StreamingInput<Tuple>, List<Attribute>> attrsMap = new HashMap<StreamingInput<Tuple>, List<Attribute>>();
	
	/**
	 * Logger
	 */
	private final static Logger logger = Logger.getLogger(AttrMetadataExtractor.class.getName());		

	public AttrMetadataExtractor(OperatorContext context) {
		this.context = context;
		init();
	}

	/**
	 * Get attributes count
	 * @return
	 */
	public int getAttrCount() {
		return attrCount;
	}
	
	/**
	 * Get attributes types list
	 * @param port - port to be considered
	 * @return - ordered list of attribute types
	 */
	public java.util.List<Type> getAttrTypesList(StreamingInput<Tuple> port) {
		return getAttrTypes(attrsMap.get(port));
	}

	
	public java.util.List<Type> getAttrTypesList(String portName) {
		for (StreamingInput<Tuple> port:  attrsMap.keySet()) {
			// the port with a given name found
			if (port.getName().equals(portName)) {
				return getAttrTypes(attrsMap.get(port));
			}
		}
		return null;
	}
	
	public java.util.List<Type> getAttrTypesList(int portNumber) {		
		for (StreamingInput<Tuple> port:  attrsMap.keySet()) {
			// the port with a given name found
			if (port.getPortNumber() == portNumber) {
				return getAttrTypes(attrsMap.get(port));
			}
		}
		return null;
	}

	private List<Type> getAttrTypes(List<Attribute> attrsList) {
		java.util.List<Type> res = new ArrayList<Type>();
		for (Attribute attr: attrsList) {
			res.add(attr.getType());
		}

		return res;
	}

	public java.util.List<String> getAttrNamesList(StreamingInput<Tuple> port) {
		return getAttrNames(attrsMap.get(port));
	}


	public java.util.List<String> getAttrNamesList(String portName) {
		for (StreamingInput<Tuple> port:  attrsMap.keySet()) {
			// the port with a given name found
			if (port.getName().equals(portName)) {
				return getAttrNames(attrsMap.get(port));
			}
		}
		return null;
	}
	
	public java.util.List<String> getAttrNamesList(int portNumber) {		
		for (StreamingInput<Tuple> port:  attrsMap.keySet()) {
			// the port with a given name found
			if (port.getPortNumber() == portNumber) {
				return getAttrNames(attrsMap.get(port));
			}
		}
		return null;
	}

	private List<String> getAttrNames(List<Attribute> attrsList) {
		java.util.List<String> res = new ArrayList<String>();
		for (Attribute attr: attrsList) {
			res.add(attr.getName());
		}

		return res;
	}

	public void init() {
		Iterator<StreamingInput<Tuple>> streamingInputIt = context.getStreamingInputs().iterator();
		StreamingInput<Tuple> currPort = null;
		StreamSchema currSchema = null;
		
		java.util.List<Attribute> attrCellList = new ArrayList<Attribute>();
		while (streamingInputIt.hasNext()) {
			currPort = streamingInputIt.next();			
			currSchema = currPort.getStreamSchema();   
			attrCount = currSchema.getAttributeCount();
		
			for (int i=0; i < attrCount;i++) {
				attrCellList.add(currSchema.getAttribute(i));
			}
			attrsMap.put(currPort, attrCellList);
		}
	}

	
}
