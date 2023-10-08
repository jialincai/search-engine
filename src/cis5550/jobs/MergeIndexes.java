package cis5550.jobs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

public class MergeIndexes {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public static void run(FlameContext fc, String[] args) {

		String index1 = args[0];
		String index2 = args[1];
		String docIdMapping1 = args[2];
		String docIdMapping2 = args[3];
		
		String outputTableName = args[4];
		String outputDocTable = args[5];
		
		System.out.println(args[0]);
		System.out.println(args[1]);
		System.out.println(args[2]);
		System.out.println(args[3]);
		System.out.println(args[4]);
		System.out.println(args[5]);
		
		KVSClient k = fc.getKVS();
		
		Iterator<Row> it1 = null;
		try {
			it1 = k.scan(index1, null, null);
		} catch (IOException e) {
			e.printStackTrace();
		}

		while (it1.hasNext()) {
			Row row1 = it1.next();
			String key = row1.key();

			try {
				// If it exists add, otherwise, add as it is. 
				
				if (k.existsRow(index2, key)) {
					Row row2 = k.getRow(index2, key);
					Set<String>cols2 = row2.columns();
					
					for (String col: cols2) {
						row1.put(col, row2.get(col));
					}
					
				} 
				
				// Add row to the output table
				k.putRow(outputTableName, row1);
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		Iterator<Row> it2 = null;
		try {
			it2 = k.scan(index2, null, null);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		while (it2.hasNext()) {
			Row row2 = it2.next();
			String key = row2.key();

			try {
				if (!k.existsRow(outputTableName, key)) {
					k.putRow(outputTableName, row2);
				} 
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		Iterator<Row> it3 = null;
		try {
			it3 = k.scan(docIdMapping1, null, null);
		} catch (IOException e) {
			e.printStackTrace();
		}

		while (it3.hasNext()) {
			Row row = it3.next();
			try {
				k.putRow(outputDocTable, row);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		Iterator<Row> it4 = null;
		try {
			it4 = k.scan(docIdMapping2, null, null);
		} catch (IOException e) {
			e.printStackTrace();
		}

		while (it4.hasNext()) {
			Row row = it4.next();
			try {
				k.putRow(outputDocTable, row);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}

}
