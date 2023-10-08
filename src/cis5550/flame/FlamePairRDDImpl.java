package cis5550.flame;

import java.util.List;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import java.util.ArrayList;
import java.util.Iterator;
import java.io.Serializable;

public class FlamePairRDDImpl implements FlamePairRDD {
	
	private String tableName;
	
	KVSClient kvs;
	
	FlamePairRDDImpl(String tableName, KVSClient kvs){
		this.tableName = tableName;
		this.kvs = kvs;
	}
	
	public String getTableName() {
		return this.tableName;
	}
	
	public List<FlamePair> collect() throws Exception {
		
		List <FlamePair> PairRDDElements = new ArrayList<FlamePair>();
		Iterator<Row> tableRows = kvs.scan(tableName);
		
		while(tableRows.hasNext()) {
			Row row = tableRows.next();
			for (String column : row.columns()) {
				FlamePair flamePair = new FlamePair(row.key(), row.get(column));
				PairRDDElements.add(flamePair);
			}
		}
		
		return PairRDDElements;
		
	}

	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		
		FlameContextImpl flameContext = new FlameContextImpl("dummyJarName.jar", kvs);
		String outputTable = flameContext.invokeOperation("/pairrdd/foldByKey", tableName, Serializer.objectToByteArray(lambda), lambda, zeroElement);
		FlamePairRDD flamePairRDD = new FlamePairRDDImpl(outputTable, kvs);
		return flamePairRDD;
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		kvs.rename(tableName, tableNameArg);
		tableName = tableNameArg;
		
	}

	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
		
		FlameContextImpl flameContext = new FlameContextImpl("dummyJarName.jar", kvs);
		String outputTable = flameContext.invokeOperation("/pairrdd/flatMap", tableName, Serializer.objectToByteArray(lambda), lambda, null);
		FlameRDD flameRDD = new FlameRDDImpl(outputTable, kvs);
		return flameRDD;
	}

	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
		
		FlameContextImpl flameContext = new FlameContextImpl("dummyJarName.jar", kvs);
		String outputTable = flameContext.invokeOperation("/pairrdd/mapToPair", tableName, Serializer.objectToByteArray(lambda), lambda, null);
		FlamePairRDD flamePairRDD = new FlamePairRDDImpl(outputTable, kvs);
		return flamePairRDD;
	}

	public FlamePairRDD join(FlamePairRDD other) throws Exception {
		
		FlameContextImpl flameContext = new FlameContextImpl("dummyJarName.jar", kvs);
		FlamePairRDDImpl otherTable = (FlamePairRDDImpl) other;
		String outputTable = flameContext.invokeOperation("/pairrdd/join", tableName, null, null, otherTable.getTableName());
		FlamePairRDD flamePairRDD = new FlamePairRDDImpl(outputTable, kvs);
		return flamePairRDD;
	}

	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
		
		FlameContextImpl flameContext = new FlameContextImpl("dummyJarName.jar", kvs);
		FlamePairRDDImpl otherTable = (FlamePairRDDImpl) other;
//		System.out.println("Other table name : " +  otherTable.getTableName());
		String outputTable = flameContext.invokeOperation("/pairrdd/cogroup", tableName, null, null, otherTable.getTableName());
		FlamePairRDD flamePairRDD = new FlamePairRDDImpl(outputTable, kvs);
		return flamePairRDD;
	}
	
}