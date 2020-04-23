package com.ibm.streamsx.parquet.test;


import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.ibm.streams.flow.declare.InputPortDeclaration;
import com.ibm.streams.flow.declare.OperatorInvocation;
import com.ibm.streams.flow.javaprimitives.JavaOperatorTester;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.parquet.ParquetSink;

/**
 * Unit test for ParquetSink operator.
 */
public class ParquetSinkTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public ParquetSinkTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( ParquetSinkTest.class );
    }

    
    
    public void testRawTableFulfillment()
    {
       	JavaOperatorTester tester = new JavaOperatorTester();    	
    	OperatorInvocation<ParquetSink> parquetSinkInvoke =  tester.singleOp(ParquetSink.class);        
    	
    	parquetSinkInvoke.setStringParameter("hdfsUri", "")
     	       .setStringParameter("rootPath", "/tmp/parquet")
     	       .setStringParameter("file", "netaction")
     	       .setIntParameter("tuplesPerFile" , 100)
     		   .setStringParameter("compressionType", "SNAPPY")
     		   .setIntParameter("blockSize", DEFAULT_BLOCK_SIZE)
     		   .setIntParameter("pageSize", DEFAULT_PAGE_SIZE)
     		   .setBooleanParameter("overwrite", true)
//     		   .setStringParameter("partitionKeyNames", "year", "month", "day", "hour")
//     		   .setStringParameter("partitionValueAttrNames", "year", "month", "day", "hour")
     		   ;
    	
    	// With a single output port
    	InputPortDeclaration parquetSinkPort = parquetSinkInvoke.addInput(ParquetTestConstants.SAMPLE_PARQUET_SINK_IN_SCHEMA);    	
    	
    	try {
			JavaTestableGraph graph = tester.tester(parquetSinkInvoke);
			graph.setTraceLevel(TraceLevel.DEBUG);	
					
			// Asynchronously initialize all the Operator invocations in the graph. 
			// When the returned Future completes successfully, all the operators in the 
			// graph have been initialized. 
			// For each operator, this results in calls to:
			// 1.Runtime context check methods
			// 2.Operator.initialize(com.ibm.streams.operator.OperatorContext)
			// After successful initialization the graph is an a running state and is
			// waiting for allPortsReady() or ExecutableGraph.execute() to be called, 
			// which allows tuple processing to start.
			Future<JavaTestableGraph> initializeNotification = graph.initialize();			
			while (!initializeNotification.isDone()) {};
			
			// Asynchronously notify all the operator invocations in the graph 
			// that the ports are ready, and the submission of tuples can start. 
			// When the returned Future completes successfully, all the operators 
			// in the graph have been notified. 
			Future<JavaTestableGraph> allPortsReadyNotification = graph.allPortsReady();
			while (!allPortsReadyNotification.isDone()) {};
			
			// Tuple submission can start			
			StreamingOutput<OutputTuple> inputTester = graph.getInputTester(parquetSinkPort);			
			for (int i = 0; i < 200; i++) {
				Map<String, RString> testTuple = generateTestTuple(i); 	
				inputTester.submitMapAsTuple(testTuple);
			}
			
			assertTrue( true );
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertTrue( false );			
		}
    }

    public void testPartitionedTableFulfillment()
    {
    	JavaOperatorTester tester = new JavaOperatorTester();    	
    	OperatorInvocation<ParquetSink> parquetSinkInvoke =  tester.singleOp(ParquetSink.class);        
    	
    	parquetSinkInvoke.setStringParameter("hdfsUri", "")
     	       .setStringParameter("rootPath", "/tmp/parquet")
     	       .setStringParameter("file", "netaction")
     	       .setIntParameter("tuplesPerFile" , 100)
     		   .setStringParameter("compressionType", "SNAPPY")
     		   .setIntParameter("blockSize", DEFAULT_BLOCK_SIZE)
     		   .setIntParameter("pageSize", DEFAULT_PAGE_SIZE)
     		   .setBooleanParameter("overwrite", true)
     		   .setStringParameter("partitionKeyNames", "year", "month", "day", "hour")
     		   .setStringParameter("partitionValueAttrNames", "YEAR", "MONTH", "DAY", "HOUR");
    	
    	// With a single output port
    	InputPortDeclaration parquetSinkPort = parquetSinkInvoke.addInput(ParquetTestConstants.SAMPLE_PARQUET_SINK_IN_SCHEMA);    	
    	
    	try {
			JavaTestableGraph graph = tester.tester(parquetSinkInvoke);
			graph.setTraceLevel(TraceLevel.DEBUG);	
					
			// Asynchronously initialize all the Operator invocations in the graph. 
			// When the returned Future completes successfully, all the operators in the 
			// graph have been initialized. 
			// For each operator, this results in calls to:
			// 1.Runtime context check methods
			// 2.Operator.initialize(com.ibm.streams.operator.OperatorContext)
			// After successful initialization the graph is an a running state and is
			// waiting for allPortsReady() or ExecutableGraph.execute() to be called, 
			// which allows tuple processing to start.
			Future<JavaTestableGraph> initializeNotification = graph.initialize();			
			while (!initializeNotification.isDone()) {};
			
			// Asynchronously notify all the operator invocations in the graph 
			// that the ports are ready, and the submission of tuples can start. 
			// When the returned Future completes successfully, all the operators 
			// in the graph have been notified. 
			Future<JavaTestableGraph> allPortsReadyNotification = graph.allPortsReady();
			while (!allPortsReadyNotification.isDone()) {};
			
			// Tuple submission can start			
			StreamingOutput<OutputTuple> inputTester = graph.getInputTester(parquetSinkPort);			
			OutputTuple oTuple = inputTester.newTuple();
			for (int i = 0; i < 200; i++) {
				Map<String, RString> testTuple = generateTestTuple(i); 	
				//inputTester.submitMapAsTuple(testTuple);
				populateOutTuple(oTuple, i);
				inputTester.submit(oTuple);
			}
			
			assertTrue( true );
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertTrue( false );			
		}
    }
    
    HashMap<String,RString> generateTestTuple(int i) {
    	HashMap<String,RString> testTuple = new HashMap<String,RString>();
    	// 2014-05-12 13:37:05.673   CID_1.1.10.78        process-79                TCP_CONNECT          END_OF_FILE          1.1.10.78              3243         576           1591602552 1.1.6.45               2923 file-54                                   572439838   2013      1      1      2
    	testTuple.put("TS", new RString("2014-05-12 13:37:05.673")); 
    	testTuple.put("COMPUTER_ID", new RString("COMP_ID" + i));
    	testTuple.put("PROCESS_NAME", new RString("firefox.exe"));
    	testTuple.put("OPERATION", new RString("OP_" + i));                         
    	testTuple.put("OPER_RESULT", new RString("OP_RES_SUCCESS" + i)); 
    	testTuple.put("SOURCE_HOST", new RString("1.1.1." + i)); 
    	testTuple.put("SOURCE_PORT", new RString("3243"));
        testTuple.put("TCP_LENGTH", new RString("74737")); 
        testTuple.put("MMS", new RString("0")); 
        testTuple.put("DEST_HOST", new RString("5.43.32." + i)); 
        testTuple.put("DEST_PORT", new RString("6564")); 
        testTuple.put("FILENAME", new RString("myfile")); 
        testTuple.put("FILE_LENGTH", new RString("8483")); 
        testTuple.put("YEAR", new RString("2014")); 
        testTuple.put("MONTH", new RString("1")); 
        //testTuple.put("day", new RString(String.valueOf(i%30))); 
        testTuple.put("DAY", new RString("1")); 
        testTuple.put("HOUR", new RString("2"));
//        testTuple.put("yearAttr", new RString("2014")); 
//        testTuple.put("monthAttr", new RString("10")); 
//        testTuple.put("dayAttr", new RString("1")); 
//        testTuple.put("hourAttr", new RString("12"));
    	
    	return testTuple;
    }
    
    void populateOutTuple(OutputTuple oTuple, int i) {
    	oTuple.setString("TS", "2014-05-12 13:37:05.673"); 
    	oTuple.setString("COMPUTER_ID", "COMP_ID" + i);
    	oTuple.setString("PROCESS_NAME", "firefox.exe");
    	oTuple.setString("OPERATION", "OP_" + i);                         
    	oTuple.setString("OPER_RESULT", "OP_RES_SUCCESS" + i); 
    	oTuple.setString("SOURCE_HOST", "1.1.1." + i); 
    	oTuple.setInt("SOURCE_PORT", 3243);
    	oTuple.setInt("TCP_LENGTH", 74737); 
    	oTuple.setInt("MMS", 0); 
    	oTuple.setString("DEST_HOST", "5.43.32." + i); 
    	oTuple.setInt("DEST_PORT", 6564); 
    	oTuple.setString("FILENAME", "myfile"); 
    	oTuple.setInt("FILE_LENGTH", 8483); 
    	oTuple.setInt("YEAR", 2014); 
    	oTuple.setInt("MONTH", 1); 
    	oTuple.setInt("DAY", 1); 
    	oTuple.setInt("HOUR", 2);
    	
    	
    }
}
