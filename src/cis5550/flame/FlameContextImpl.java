package cis5550.flame;

import java.io.*;
import java.util.*;
import java.net.*;

import cis5550.kvs.KVSClient;
import cis5550.tools.*;
import cis5550.tools.Partitioner.Partition;

public class FlameContextImpl implements FlameContext {
    protected boolean hasOutput_;
    protected String outputString_;
    protected String jarName_;
    protected int concurrencyLevel_;

    public FlameContextImpl(String jarName) throws IOException {
        hasOutput_ = false;
        outputString_ = "No Output";
        jarName_ = jarName;
        concurrencyLevel_ = 1;
    }

    public String finalOutput() {
        return outputString_;
    }

    @Override
    public KVSClient getKVS() {
        return Master.kvs;
    }

    @Override
    public void output(String s) {
        if (!hasOutput_) {
            hasOutput_ = true;
            outputString_ = "";
        }
        outputString_ += s;
    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        String tableKey = String.format("%s:%s",
                Long.toString(System.currentTimeMillis()), Integer.toString(Master.nextJobID));

        for (int i = 0; i < list.size(); i++) {
            String rowKey = Hasher.hash(Integer.toString(i));
            Master.kvs.put(tableKey, rowKey, "value", list.get(i).getBytes());
        }

        return new FlameRDDImpl(tableKey, this);
    }

    public Vector<Partition> getPartitions() throws IOException {
        Partitioner partitioner = new Partitioner();
        partitioner.setKeyRangesPerWorker(concurrencyLevel_);
        for (int i = 0; i < Master.kvs.numWorkers() - 1; i++) {
            partitioner.addKVSWorker(Master.kvs.getWorkerAddress(i), Master.kvs.getWorkerID(i),
                    Master.kvs.getWorkerID(i + 1));
        }
        partitioner.addKVSWorker(Master.kvs.getWorkerAddress(Master.kvs.numWorkers() - 1),
                null, Master.kvs.getWorkerID(0));
        partitioner.addKVSWorker(Master.kvs.getWorkerAddress(Master.kvs.numWorkers() - 1),
                Master.kvs.getWorkerID(Master.kvs.numWorkers() - 1), null);
        for (String flameWorker : Master.getWorkers()) {
            partitioner.addFlameWorker(flameWorker);
        }
        return partitioner.assignPartitions();
    }

    public String invokeOperation(String opURL, byte[] lambda, String inTableKey, String argument)
            throws Exception {
        String outTableKey = String.format("%s:%s",
                Long.toString(System.currentTimeMillis()), Integer.toString(Master.nextJobID));

        Vector<Partition> partitions = getPartitions();

        // Send HTTP request to FlameWorkers.
        Thread threads[] = new Thread[partitions.size()];
        String results[] = new String[partitions.size()];
        for (int i = 0; i < partitions.size(); i++) {
            Partition partition = partitions.get(i);

            StringBuilder urlBuilder = new StringBuilder("http://" + partition.assignedFlameWorker + opURL);
            if (partition.fromKey != null) {
                urlBuilder.append("?start=" + URLEncoder.encode(partition.fromKey, "UTF-8"));
            }
            if (partition.toKeyExclusive != null) {
                urlBuilder.append("?end=" + URLEncoder.encode(partition.toKeyExclusive, "UTF-8"));
            }
            urlBuilder.append("&in=" + URLEncoder.encode(inTableKey, "UTF-8"))
                    .append("&out=" + URLEncoder.encode(outTableKey, "UTF-8"))
                    .append("&kvs=" + URLEncoder.encode(Master.kvs.getMaster(), "UTF-8"));
            if (argument != null) {
                urlBuilder.append("&arg=" + URLEncoder.encode(argument, "UTF-8"));
            }

            final String url = urlBuilder.toString();
            final int j = i;
            threads[i] = new Thread("Operation Invocation #" + (i + 1)) {
                public void run() {
                    try {
                        results[j] = new String(HTTP.doRequest("POST", url, lambda).body());
                    } catch (Exception e) {
                        results[j] = "Exception: " + e;
                        e.printStackTrace();
                    }
                }
            };
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
                if (!results[i].equals("OK")) {
                    throw new Exception("Error from FlameWorker: " + results[i]);
                }
            } catch (InterruptedException ie) {
            }
        }

        return outTableKey;
    }

    @Override
    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        String resTable = this.invokeOperation("/context/fromTable", Serializer.objectToByteArray(lambda),
                new String(tableName), null);
        return new FlameRDDImpl(resTable, this);
    }

    @Override
    public void setConcurrencyLevel(int keyRangesPerWorker) {
        concurrencyLevel_ = keyRangesPerWorker;
    }
}
