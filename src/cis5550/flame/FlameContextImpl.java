package cis5550.flame;

import java.util.*;
import java.io.*;
import cis5550.kvs.KVSClient;
import cis5550.tools.*;
import cis5550.tools.HTTP.Response;
import cis5550.tools.Partitioner.Partition;

import cis5550.flame.*;
import cis5550.webserver.*;

public class FlameContextImpl implements FlameContext {
	
	public String outputBody = "";
	
	private int sequenceNumber = 1;
	
	private long rowSequenceNumber = 1;
	
	private int outTabSeqNum = 1;
	
	private String foldAccumulator;
	
	KVSClient kvs;
	
	String jarName;
	
	FlameContextImpl(String jarName, KVSClient kvs){
		this.jarName = jarName;
		this.kvs = kvs;
	}

	public KVSClient getKVS() {
		// TODO Auto-generated method stub
		return kvs;
	}

	public void output(String s) {
		outputBody += s;
	}
	
	public String getOutputBody() {
		if (outputBody.equals("")) {
			return "No output body found!"; 
		}
		return outputBody;
	}

	public FlameRDD parallelize(List<String> list) throws Exception {
		
		String jobID = Long.toString(System.currentTimeMillis()) + "_" + sequenceNumber; // tableName 
		sequenceNumber += 1;
		
		for (String columnName : list) {
			String rowKey = Hasher.hash(Long.toString(rowSequenceNumber));
			kvs.put(jobID, rowKey, "value", columnName.getBytes());
			rowSequenceNumber += 1;
		}
		
		FlameRDDImpl flameRDD = new FlameRDDImpl(jobID, kvs);
		
		return flameRDD;
	}
	
	public class WorkerRequest implements Runnable {
		
		String opName;
		String workerAddress;
		String inputTable;
		String outputTable;
		byte[] lambda;
		String keyLow; 
		String keyHigh; 
		String kvsMasterAddress;
		String zeroElement;
		Object oriLambda;
		
		WorkerRequest(String opName, String workerAddress, String inputTable, String outputTable, byte[] lambda, Object oriLambda, String keyLow, String keyHigh, String kvsMasterAddress, String zeroElement){
			this.opName = opName;
			this.workerAddress = workerAddress;
			this.inputTable = inputTable;
			this.outputTable = outputTable;
			this.lambda = lambda;
			this.keyLow = keyLow;
			this.keyHigh = keyHigh;
			this.kvsMasterAddress = kvsMasterAddress;
			this.zeroElement = zeroElement;
			this.oriLambda = oriLambda;
		}
		
        public void run() {
        	try {
        		String urlArg = String.format("http://%s%s?intable=%s&outtable=%s&keylow=%s&keyhigh=%s&kvsmaster=%s", workerAddress, opName, inputTable, outputTable, keyLow, keyHigh, kvsMasterAddress);
        		if (opName.equals("/pairrdd/foldByKey")) {
        			urlArg += "&zeroElement=" + zeroElement;
        		} 
        		if(opName.equals("/rdd/sample")) {
        			urlArg += "&proba=" + zeroElement;
        		}
        		if(opName.equals("/pairrdd/join")) {
        			urlArg += "&otherTable=" + zeroElement;
        		}
        		if (opName.equals("/rdd/fold")) {
        			urlArg += "&zeroElement=" + zeroElement;
        		}
        		if(opName.equals("/pairrdd/cogroup")) {
        			urlArg += "&otherTable=" + zeroElement;
        		}
//        		System.out.println("Making request " + urlArg);
        		Response response = HTTP.doRequest("POST", urlArg, lambda);
        		
        		if (opName.equals("/rdd/fold")) {
        			
        			File myJAR = Worker.myJAR;
        			FlamePairRDD.TwoStringsToString oriLambda_ = (FlamePairRDD.TwoStringsToString) oriLambda;
//        			FlamePairRDD.TwoStringsToString deserializedLambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(lambda, myJAR);
        			synchronized(foldAccumulator) {
        				
        				foldAccumulator = oriLambda_.op(foldAccumulator, new String(response.body()));
        			}
        			
        		}
        		if(response.statusCode() != 200) {
        			
        		}
			} catch (IOException e) {
				System.out.println("IOException while sending a POST request to worker in invokeOperation.");
				e.printStackTrace();
			}
        }
    };
	
	public String invokeOperation(String opName, String inputTable, byte[] lambda, Object oriLambda, String zeroElement) throws IOException {
		
		String outputTable = Long.toString(System.currentTimeMillis()) + "_" + outTabSeqNum; // tableName  
		outTabSeqNum += 1;
		
		Partitioner partitioner = new Partitioner();

		// Adding KVS Workers 
		int numKvsWorkers = kvs.numWorkers(); 
		
		System.out.println("numKvsWorkers="+numKvsWorkers);
		
		
		
		for (int wIdx = 0; wIdx < numKvsWorkers; wIdx++) {
			String wAddress = kvs.getWorkerAddress(wIdx);
			String wId = kvs.getWorkerID(wIdx);
			
			partitioner.addKVSWorker(wAddress, null, null);

//			if (wIdx+1 < numKvsWorkers) {
//				partitioner.addKVSWorker(wAddress, wId, kvs.getWorkerID(wIdx+1));
//			} else {
//				partitioner.addKVSWorker(wAddress, wId, null);
//				partitioner.addKVSWorker(wAddress, null, kvs.getWorkerID(0));
//			}
		}
		
		// Adding Flame Workers
		List<String> flameWorkers = Master.getWorkers();
		for (String flameWorker : flameWorkers)
			partitioner.addFlameWorker(flameWorker);
		
		// Assigning partitions
		Vector<Partition> partitions = partitioner.assignPartitions();
		
		// Parallely send messages to all workers		
		Vector<Thread> workerThreads = new Vector<Thread>();
		
		if (opName.equals("/rdd/fold")) {
			foldAccumulator = zeroElement;
		}
		
		for (Partition partition: partitions) {

		    Runnable workerRequest = new WorkerRequest(opName, partition.assignedFlameWorker, inputTable, outputTable, lambda, oriLambda, partition.fromKey, partition.toKeyExclusive, kvs.getMaster(), zeroElement);
				
			Thread t = new Thread(workerRequest);
			t.start();
			workerThreads.add(t);

		}
		
		for (Thread worker: workerThreads) {
			try {
				worker.join(); // TODO: check whether any requests failed or returned status codes other than 200. If so, your function should report this to the caller.
			} catch (InterruptedException e) {
				System.out.println("InterruptedException while waiting for a worker thread to finish in invokeOperation.");
				e.printStackTrace();
			}
		}
		
		if (opName.equals("/rdd/fold")) {
			return foldAccumulator;
		}
		
		return outputTable;
			
	}

	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
		
		String outputTable = invokeOperation("/rdd/fromTable", tableName, Serializer.objectToByteArray(lambda), null, null);
		FlameRDD flameRDD = new FlameRDDImpl(outputTable, kvs);
		return flameRDD;
	}

	public void setConcurrencyLevel(int keyRangesPerWorker) {
		// TODO Auto-generated method stub
		
	}

}