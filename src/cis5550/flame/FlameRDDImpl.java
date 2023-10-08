package cis5550.flame;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import cis5550.kvs.*;
import cis5550.tools.Serializer;
import cis5550.flame.FlameContext.*;
import cis5550.flame.FlamePairRDD.TwoStringsToString;

public class FlameRDDImpl implements FlameRDD {
	
	private String tableName;
	
	KVSClient kvs;
	
	FlameRDDImpl(String tableName, KVSClient kvs){
		this.tableName = tableName;
		this.kvs = kvs;
	}

	public List<String> collect() throws Exception {
		
		List <String> RDDElements = new ArrayList<String>();
		Iterator<Row> tableRows = kvs.scan(tableName);
		
		while(tableRows.hasNext()) {
			Row row = tableRows.next();
			RDDElements.add(row.get("value"));
		}
		
		return RDDElements;
	}

	public FlameRDD flatMap(StringToIterable lambda) throws Exception {
		
		FlameContextImpl flameContext = new FlameContextImpl("dummyJarName.jar", kvs);
		String outputTable = flameContext.invokeOperation("/rdd/flatMap", tableName, Serializer.objectToByteArray(lambda), lambda, null);
		FlameRDD flameRDD = new FlameRDDImpl(outputTable, kvs);
		return flameRDD;	
	}

	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		
		FlameContextImpl flameContext = new FlameContextImpl("dummyJarName.jar", kvs);
		String outputTable = flameContext.invokeOperation("/rdd/mapToPair", tableName, Serializer.objectToByteArray(lambda), lambda, null);
		FlamePairRDD flamePairRDD = new FlamePairRDDImpl(outputTable, kvs);
		return flamePairRDD;
	}

	public FlameRDD intersection(FlameRDD r) throws Exception {
		
		return null;

	}

	public FlameRDD sample(double f) throws Exception {
		
		FlameContextImpl flameContext = new FlameContextImpl("dummyJarName.jar", kvs);
		String outputTable = flameContext.invokeOperation("/rdd/sample", tableName, null, null, Double.toString(f));
		FlameRDD flameRDD = new FlameRDDImpl(outputTable, kvs);
		return flameRDD;
	}

	public FlamePairRDD groupBy(StringToString lambda) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	public int count() throws Exception {
//		System.out.println("Returning count of table : " + kvs.count(tableName));
		return kvs.count(tableName); 
	}

	public void saveAsTable(String tableNameArg) throws Exception {
		kvs.rename(tableName, tableNameArg);
		tableName = tableNameArg;	
	}

	public FlameRDD distinct() throws Exception {
		
		String outputTable = Long.toString(System.currentTimeMillis()) + (new Random());
		FlameRDD flameRDD = new FlameRDDImpl(outputTable, kvs);
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(tableName);
		
		while(eligibleRows.hasNext()) {
			
			Row row = eligibleRows.next();	
			kvs.put(outputTable, row.get("value"), "value", row.get("value"));			
		}
		
		return flameRDD;
	}

	public Vector<String> take(int num) throws Exception {
		
		Vector<String> result = new Vector<String> ();
		
		Iterator<Row> eligibleRows =  (Iterator<Row>)kvs.scan(tableName);
		
		while(eligibleRows.hasNext() && result.size() < num) {
			
			Row row = eligibleRows.next();	
			result.add(row.get("value"));			
		}
		
		return result;
	}


	public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
		
		FlameContextImpl flameContext = new FlameContextImpl("dummyJarName.jar", kvs);
		String output = flameContext.invokeOperation("/rdd/fold", tableName, Serializer.objectToByteArray(lambda), lambda, zeroElement);
		return output;
	}

	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {

		FlameContextImpl flameContext = new FlameContextImpl("dummyJarName.jar", kvs);
		String outputTable = flameContext.invokeOperation("/rdd/flatMapToPair", tableName, Serializer.objectToByteArray(lambda), lambda, null);
		FlamePairRDD flamePairRDD = new FlamePairRDDImpl(outputTable, kvs);
		return flamePairRDD;
	}

	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		
		FlameContextImpl flameContext = new FlameContextImpl("dummyJarName.jar", kvs);
		String outputTable = flameContext.invokeOperation("/rdd/filter", tableName, Serializer.objectToByteArray(lambda), lambda, null);
		FlameRDD flameRDD = new FlameRDDImpl(outputTable, kvs);
		return flameRDD;
	}

	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		
		FlameContextImpl flameContext = new FlameContextImpl("dummyJarName.jar", kvs);
		String outputTable = flameContext.invokeOperation("/rdd/mapPartitions", tableName, Serializer.objectToByteArray(lambda), lambda, null);
		FlameRDD flameRDD = new FlameRDDImpl(outputTable, kvs);
		return flameRDD;
	}
	
}