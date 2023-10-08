package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.kvs.*;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

class Worker extends cis5550.generic.Worker {
	
	public static File myJAR;

	private static String getFlatMap(Request request, Response response, File myJAR) throws Exception {
			
		String inputTable = request.queryParams("intable");
		String outputTable = request.queryParams("outtable");
		String keyLow = request.queryParams("keylow");
		String keyHigh = request.queryParams("keyhigh");
		String kvsMasterAddress = request.queryParams("kvsmaster");
		
		if (keyLow.equals("null"))
			keyLow = null;
		
		if (keyHigh.equals("null"))
			keyHigh = null;
		
		byte[] lambda = request.bodyAsBytes();
		FlameRDD.StringToIterable deserializedLambda = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(lambda, myJAR);
		
		KVSClient kvs = new KVSClient(kvsMasterAddress);
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(inputTable, keyLow, keyHigh);
		
		UUID uid = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d"); 
		while(eligibleRows.hasNext()) {
			
			Row row = eligibleRows.next();
			
			Iterable<String> result =  deserializedLambda.op(row.get("value"));
			if (result != null) {
				for (String res : result) {
					String rowKey = System.currentTimeMillis() + "_" + uid.randomUUID();
					kvs.put(outputTable, rowKey, "value", res);
				}
			}		
		}

		return null;
		 
	}
	
	
	private static String convertMapToPair(Request request, Response response, File myJAR) throws Exception {
		
		String inputTable = request.queryParams("intable");
		String outputTable = request.queryParams("outtable");
		String keyLow = request.queryParams("keylow");
		String keyHigh = request.queryParams("keyhigh");
		String kvsMasterAddress = request.queryParams("kvsmaster");
		
		if (keyLow.equals("null"))
			keyLow = null;
		
		if (keyHigh.equals("null"))
			keyHigh = null;
		
		byte[] lambda = request.bodyAsBytes();
		FlameRDD.StringToPair deserializedLambda = (FlameRDD.StringToPair) Serializer.byteArrayToObject(lambda, myJAR);
		
		KVSClient kvs = new KVSClient(kvsMasterAddress);
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(inputTable, keyLow, keyHigh);
		
		while(eligibleRows.hasNext()) {
			
			Row row = eligibleRows.next();
			
			Set<String> columns = row.columns();
			for (String column : columns) {
				FlamePair result =  deserializedLambda.op(row.get(column));
				if (result != null) {
					kvs.put(outputTable, result._1(), row.key(), result._2());
				}
			}
		}
		
		return null;
	}
	
	
	private static String foldByKey(Request request, Response response, File myJAR) throws Exception {
		
		String inputTable = request.queryParams("intable");
		String outputTable = request.queryParams("outtable");
		String keyLow = request.queryParams("keylow");
		String keyHigh = request.queryParams("keyhigh");
		String kvsMasterAddress = request.queryParams("kvsmaster");
		String zeroElement = request.queryParams("zeroElement");
		
		if (keyLow.equals("null"))
			keyLow = null;
		
		if (keyHigh.equals("null"))
			keyHigh = null;
		
		byte[] lambda = request.bodyAsBytes();
		FlamePairRDD.TwoStringsToString deserializedLambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(lambda, myJAR);
		
		KVSClient kvs = new KVSClient(kvsMasterAddress);
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(inputTable, keyLow, keyHigh);
		
		while(eligibleRows.hasNext()) {
			
			Row row = eligibleRows.next();
			
			Set<String> columns = row.columns();
			String outputAccumulator = zeroElement;
			
			for (String column : columns) {			
				outputAccumulator = deserializedLambda.op(outputAccumulator, row.get(column)); 
			}
						
			kvs.put(outputTable, row.key(), "value", outputAccumulator);
		}
		
		return null;
	}
	
	
	private static String sampleElements(Request request, Response response, File myJAR) throws Exception {
		
		String inputTable = request.queryParams("intable");
		String outputTable = request.queryParams("outtable");
		String keyLow = request.queryParams("keylow");
		String keyHigh = request.queryParams("keyhigh");
		String kvsMasterAddress = request.queryParams("kvsmaster");
		double probaThresh = Double.parseDouble(request.queryParams("zeroElement"));
		
		
		if (keyLow.equals("null"))
			keyLow = null;
		
		if (keyHigh.equals("null"))
			keyHigh = null;
		
		
		KVSClient kvs = new KVSClient(kvsMasterAddress);
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(inputTable, keyLow, keyHigh);
		
		while(eligibleRows.hasNext()) {
			
			Row row = eligibleRows.next();
			
			Set<String> columns = row.columns();
			
			for (String column : columns) {	
				if (new Random().nextDouble() < probaThresh) {
					kvs.put(outputTable, row.key(), column, row.get(column));
				}
			}
							
		}
		
		return null;
	}
	
	
	private static String fromTable(Request request, Response response, File myJAR) throws Exception {
		
		String inputTable = request.queryParams("intable");
		String outputTable = request.queryParams("outtable");
		String keyLow = request.queryParams("keylow");
		String keyHigh = request.queryParams("keyhigh");
		String kvsMasterAddress = request.queryParams("kvsmaster");
		
		if (keyLow.equals("null"))
			keyLow = null;
		
		if (keyHigh.equals("null"))
			keyHigh = null;
		
		System.out.println("inputTable="+inputTable);
		System.out.println("keyLow="+keyLow);
		System.out.println("keyHigh="+keyHigh);
		
		byte[] lambda = request.bodyAsBytes();
		FlameContext.RowToString deserializedLambda = (FlameContext.RowToString) Serializer.byteArrayToObject(lambda, myJAR);
		
		KVSClient kvs = new KVSClient(kvsMasterAddress);
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(inputTable, keyLow, keyHigh);
		
		System.out.println("Scanning rows from: " + keyLow + " -> " + keyHigh); 
		
		UUID uid = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d"); 
		
		int ptr = 0;
		while(eligibleRows.hasNext()) {
			
			Row row = eligibleRows.next();
			ptr += 1;
			String result =  deserializedLambda.op(row);
			if (result != null) {
				String rowKey = System.currentTimeMillis() + "_" + uid.randomUUID();
				kvs.put(outputTable, rowKey, "value", result);
			}		
		}
		
		System.out.println("ptr="+ptr);
		

		return null;
		 
	} 
	
	
	private static String getFlatMapToPair(Request request, Response response, File myJAR) throws Exception {
		
		String inputTable = request.queryParams("intable");
		String outputTable = request.queryParams("outtable");
		String keyLow = request.queryParams("keylow");
		String keyHigh = request.queryParams("keyhigh");
		String kvsMasterAddress = request.queryParams("kvsmaster");
		
		if (keyLow.equals("null"))
			keyLow = null;
		
		if (keyHigh.equals("null"))
			keyHigh = null;
		
		byte[] lambda = request.bodyAsBytes();
		FlameRDD.StringToPairIterable deserializedLambda = (FlameRDD.StringToPairIterable) Serializer.byteArrayToObject(lambda, myJAR);
		
		KVSClient kvs = new KVSClient(kvsMasterAddress);
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(inputTable, keyLow, keyHigh);
		UUID uid = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d"); 
		
		while(eligibleRows.hasNext()) {
			
			Row row = eligibleRows.next();
			
			Iterable<FlamePair> result =  deserializedLambda.op(row.get("value"));
			if (result != null) {
				for (FlamePair res : result) {
					
					if (res != null) {
						String colKey = System.currentTimeMillis() + "_" + uid.randomUUID();
						kvs.put(outputTable, res._1(), colKey, res._2());
					}
				}
			}		
		}

		return null;
		 
	}

	
	private static String getPairRDDFlatMap(Request request, Response response, File myJAR) throws Exception {
		
		String inputTable = request.queryParams("intable");
		String outputTable = request.queryParams("outtable");
		String keyLow = request.queryParams("keylow");
		String keyHigh = request.queryParams("keyhigh");
		String kvsMasterAddress = request.queryParams("kvsmaster");
		
		if (keyLow.equals("null"))
			keyLow = null;
		
		if (keyHigh.equals("null"))
			keyHigh = null;
		
		byte[] lambda = request.bodyAsBytes();
		FlamePairRDD.PairToStringIterable deserializedLambda = (FlamePairRDD.PairToStringIterable) Serializer.byteArrayToObject(lambda, myJAR);
		
		KVSClient kvs = new KVSClient(kvsMasterAddress);
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(inputTable, keyLow, keyHigh);
		
		UUID uid = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d"); 
		while(eligibleRows.hasNext()) {
			
			Row row = eligibleRows.next();
			Set<String> columns = row.columns();
			
			for (String column : columns) {	
				
				Iterable<String> result =  deserializedLambda.op(new FlamePair(row.key(), row.get(column)));
				if (result != null) {
					for (String res : result) {
						String rowKey = System.currentTimeMillis() + "_" + uid.randomUUID();
						kvs.put(outputTable, rowKey, "value", res);
					}
				}
			}	
			
		}

		return null;
		 
	}

	
	private static String getPairRDDFlatMapToPair(Request request, Response response, File myJAR) throws Exception {
		
		String inputTable = request.queryParams("intable");
		String outputTable = request.queryParams("outtable");
		String keyLow = request.queryParams("keylow");
		String keyHigh = request.queryParams("keyhigh");
		String kvsMasterAddress = request.queryParams("kvsmaster");
		
		if (keyLow.equals("null"))
			keyLow = null;
		
		if (keyHigh.equals("null"))
			keyHigh = null;
		
		byte[] lambda = request.bodyAsBytes();
		FlamePairRDD.PairToPairIterable deserializedLambda = (FlamePairRDD.PairToPairIterable) Serializer.byteArrayToObject(lambda, myJAR);
		
		KVSClient kvs = new KVSClient(kvsMasterAddress);
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(inputTable, keyLow, keyHigh);
		UUID uid = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d"); 
		
		while(eligibleRows.hasNext()) {
			
			Row row = eligibleRows.next();
			Set<String> columns = row.columns();
			
			for (String column : columns) {	
				
				Iterable<FlamePair> result =  deserializedLambda.op(new FlamePair(row.key(), row.get(column)));
				
				if (result != null) {
					for (FlamePair res : result) {
						
						if (res != null) {
							String colKey = System.currentTimeMillis() + "_" + uid.randomUUID();
							kvs.put(outputTable, res._1(), colKey, res._2());
						}
					}
				}
			}
		}

		return null;
		 
	}

	
	private static String joinTables(Request request, Response response, File myJAR) throws Exception {
		
		String inputTable = request.queryParams("intable");
		String outputTable = request.queryParams("outtable");
		String keyLow = request.queryParams("keylow");
		String keyHigh = request.queryParams("keyhigh");
		String kvsMasterAddress = request.queryParams("kvsmaster");
		String inputTable2 = request.queryParams("otherTable");
		
		if (keyLow.equals("null"))
			keyLow = null;
		
		if (keyHigh.equals("null"))
			keyHigh = null;
		
//		byte[] lambda = request.bodyAsBytes();
//		FlameRDD.StringToIterable deserializedLambda = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(lambda, myJAR);
		
		KVSClient kvs = new KVSClient(kvsMasterAddress);
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(inputTable, keyLow, keyHigh);
		
		UUID uid = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d");
		
		while(eligibleRows.hasNext()) {
			
			Row row = eligibleRows.next();
			
//			System.out.println("Checking for row " + row.key() + " in second table");
			
			if (kvs.existsRow(inputTable2, row.key())) {
				
//				System.out.println("Row " + row.key() + " exists in both tables");
				
				Row row2 = kvs.getRow(inputTable2, row.key());
				for (String column1 : row.columns()) {
					for (String column2 : row2.columns()) {
						if (row.get(column1) != null && row2.get(column2) != null) {
							String colKey = System.currentTimeMillis() + "_" + uid.randomUUID();
							kvs.put(outputTable, row.key(), colKey, row.get(column1) + "," + row2.get(column2));
						}
					}
				}
			}
						
		}

		return null;
		 
	}
	
	
	private static String RDDfold(Request request, Response response, File myJAR) throws Exception {
		
		String inputTable = request.queryParams("intable");
		String outputTable = request.queryParams("outtable");
		String keyLow = request.queryParams("keylow");
		String keyHigh = request.queryParams("keyhigh");
		String kvsMasterAddress = request.queryParams("kvsmaster");
		String zeroElement = request.queryParams("zeroElement");
		
		if (keyLow.equals("null"))
			keyLow = null;
		
		if (keyHigh.equals("null"))
			keyHigh = null;
		
		byte[] lambda = request.bodyAsBytes();
		FlamePairRDD.TwoStringsToString deserializedLambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(lambda, myJAR);
		
		KVSClient kvs = new KVSClient(kvsMasterAddress);
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(inputTable, keyLow, keyHigh);

		String outputAccumulator = zeroElement;
		
		while(eligibleRows.hasNext()) {
			
			Row row = eligibleRows.next();			
			Set<String> columns = row.columns();
			
			for (String column : columns) {			
				outputAccumulator = deserializedLambda.op(outputAccumulator, row.get(column)); 
			}
		}
		
		return outputAccumulator;
	}
	

	private static String filter(Request request, Response response, File myJAR) throws Exception {
		
		String inputTable = request.queryParams("intable");
		String outputTable = request.queryParams("outtable");
		String keyLow = request.queryParams("keylow");
		String keyHigh = request.queryParams("keyhigh");
		String kvsMasterAddress = request.queryParams("kvsmaster");
		
		if (keyLow.equals("null"))
			keyLow = null;
		
		if (keyHigh.equals("null"))
			keyHigh = null;
		
		byte[] lambda = request.bodyAsBytes();
		FlameRDD.StringToBoolean deserializedLambda = (FlameRDD.StringToBoolean) Serializer.byteArrayToObject(lambda, myJAR);
		
		KVSClient kvs = new KVSClient(kvsMasterAddress);
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(inputTable, keyLow, keyHigh);
		
		UUID uid = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d"); 
		
		while(eligibleRows.hasNext()) {
			
			Row row = eligibleRows.next();			
			boolean result =  deserializedLambda.op(row.get("value"));
			
			if (result) {
				kvs.put(outputTable, row.key(), "value", row.get("value"));
			}		
		}
		
		return null;
	}
	
	
	private static String mapPartitions(Request request, Response response, File myJAR) throws Exception {
		
		String inputTable = request.queryParams("intable");
		String outputTable = request.queryParams("outtable");
		String keyLow = request.queryParams("keylow");
		String keyHigh = request.queryParams("keyhigh");
		String kvsMasterAddress = request.queryParams("kvsmaster");
		
		if (keyLow.equals("null"))
			keyLow = null;
		
		if (keyHigh.equals("null"))
			keyHigh = null;
		
		byte[] lambda = request.bodyAsBytes();
		FlameRDD.IteratorToIterator deserializedLambda = (FlameRDD.IteratorToIterator) Serializer.byteArrayToObject(lambda, myJAR);
		
		KVSClient kvs = new KVSClient(kvsMasterAddress);
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(inputTable, keyLow, keyHigh);
		
		List<String> eligibleElementsList = new ArrayList<String> ();

		while (eligibleRows.hasNext()) {
			
			eligibleElementsList.add(eligibleRows.next().get("value"));
		}
		
		Iterator<String> eligibleElements = eligibleElementsList.iterator();
		
		Iterator<String> resultRows = deserializedLambda.op(eligibleElements);
		
		UUID uid = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d"); 
		
		while(resultRows.hasNext()) {
			
			String rowKey = System.currentTimeMillis() + "_" + uid.randomUUID();
			kvs.put(outputTable, rowKey, "value", resultRows.next());			
		}
		
		return null;
	}

	
	private static String getCogroup(Request request, Response response, File myJAR) throws Exception {
		
		String inputTable = request.queryParams("intable");
		String outputTable = request.queryParams("outtable");
		String keyLow = request.queryParams("keylow");
		String keyHigh = request.queryParams("keyhigh");
		String kvsMasterAddress = request.queryParams("kvsmaster");
		String inputTable2 = request.queryParams("otherTable");
		
		if (keyLow.equals("null"))
			keyLow = null;
		
		if (keyHigh.equals("null"))
			keyHigh = null;
		
		KVSClient kvs = new KVSClient(kvsMasterAddress);
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(inputTable, keyLow, keyHigh);
		
		UUID uid = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d"); 
		
		while(eligibleRows.hasNext()) {
			
			Row row = eligibleRows.next();
			
			String table1vals = "[";
			String table2vals = "[";
		
			for (String column1 : row.columns()) 
				table1vals += row.get(column1) + ",";

			if (kvs.existsRow(inputTable2, row.key())) {
				
				Row row2 = kvs.getRow(inputTable2, row.key());
				
				for (String column2 : row2.columns()) 
					table2vals += row2.get(column2) + ",";
			}  
			
			if(table1vals.endsWith(",")) 
				table1vals = table1vals.substring(0, table1vals.length() - 1);
			
			table1vals += "]";
			
			if(table2vals.endsWith(",")) 
				table2vals = table2vals.substring(0, table2vals.length() - 1);
			
			table2vals += "]";	
			
			String finalVal = table1vals + "," + table2vals;
			
			String colKey = System.currentTimeMillis() + "_" + uid.randomUUID();
			kvs.put(outputTable, row.key(), colKey, finalVal.getBytes());
		}
		
		Iterator<Row> eligibleRows2 =  (Iterator<Row>)kvs.scan(inputTable2, keyLow, keyHigh);
		
		while(eligibleRows2.hasNext()) {
			
			Row row = eligibleRows2.next();
			
			if (!kvs.existsRow(inputTable, row.key())) {
				
				String table1vals = "[]";
				String table2vals = "[";
				
				for (String column : row.columns()) 
					table2vals += row.get(column) + ",";
				
				if(table2vals.endsWith(",")) 
					table2vals = table2vals.substring(0, table2vals.length() - 1);
				
				table2vals += "]";
				
				String finalVal = table1vals + "," + table2vals;
				
				String colKey = System.currentTimeMillis() + "_" + uid.randomUUID();
				kvs.put(outputTable, row.key(), colKey, finalVal.getBytes());				
			}
		}
		
		return null;		 
	}
	
	
	public static void main(String args[]) {
    if (args.length != 2) {
    	System.err.println("Syntax: Worker <port> <masterIP:port>");
    	System.exit(1);
    }
    
    System.out.println("Sleeping for 12 seconds...");
    try {
        Thread.sleep(12000); // sleep for 10 seconds (10,000 milliseconds)
    } catch (InterruptedException e) {
        e.printStackTrace();
    }
    System.out.println("Done sleeping.");

    int port = Integer.parseInt(args[0]);

    String server = args[1];
    startPingThread(port,"flame"+port,server);
//	  startPingThread(Integer.parseInt(server), ""+port, port;
    myJAR = new File("__worker"+port+"-current.jar");

  	port(port);

    post("/useJAR", (request,response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });
    
    post("/rdd/flatMap", (request,response) -> {return getFlatMap(request, response, myJAR);});
    
    post("/rdd/mapToPair", (request, response) -> {return convertMapToPair(request, response, myJAR);});
    
    post("/pairrdd/foldByKey", (request, response) -> {return foldByKey(request, response, myJAR);});
    
    post("/rdd/sample", (request, response) -> {return sampleElements(request, response, myJAR);}); // Failed tests
    
    post("/rdd/fromTable", (request,response) -> {return fromTable(request, response, myJAR);});
    
    post("/rdd/flatMapToPair", (request,response) -> {return getFlatMapToPair(request, response, myJAR);});
    
    post("/pairrdd/flatMap", (request,response) -> {return getPairRDDFlatMap(request, response, myJAR);});
    
    post("/pairrdd/mapToPair", (request,response) -> {return getPairRDDFlatMapToPair(request, response, myJAR);});
    
    post("/pairrdd/join", (request, response) -> {return joinTables(request, response, myJAR);});
    
    post("/rdd/fold", (request, response) -> {return RDDfold(request, response, myJAR);});
    
    post("/rdd/filter", (request, response) -> {return filter(request, response, myJAR);});
    
    post("/rdd/mapPartitions", (request, response) -> {return mapPartitions(request, response, myJAR);});
    
    post("/pairrdd/cogroup", (request, response) -> {return getCogroup(request, response, myJAR);});

	}
}
