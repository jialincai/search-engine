package cis5550.flame;

import java.nio.charset.StandardCharsets;
import java.util.*;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.*;
import cis5550.tools.Serializer;

public class FlameRDDImpl implements FlameRDD {
    String tableName_;
    FlameContextImpl context_;

    public FlameRDDImpl(String tableName, FlameContextImpl context) {
        tableName_ = tableName;
        context_ = context;
    }

    @Override
    public List<String> collect() throws Exception {
        List<String> allValues = new LinkedList<>();
        Iterator<Row> iter = Master.kvs.scan(tableName_);

        while (iter.hasNext()) {
            Row r = iter.next();
            allValues.add(r.get("value"));
        }

        return allValues;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        String resTable = context_.invokeOperation("/rdd/flatMap", Serializer.objectToByteArray(lambda),
                new String(tableName_), null);
        return new FlameRDDImpl(resTable, context_);
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        String resTable = context_.invokeOperation("/rdd/mapToPair", Serializer.objectToByteArray(lambda),
                new String(tableName_), null);
        return new FlamePairRDDImpl(resTable, context_);
    }

    @Override
    public int count() throws Exception {
        return Master.kvs.count(tableName_);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        Master.kvs.rename(tableName_, tableNameArg);
        tableName_ = tableNameArg;
    }

    @Override
    public FlameRDD distinct() throws Exception {
        String resTable = context_.invokeOperation("/rdd/distinct", null,
                new String(tableName_), null);
        return new FlameRDDImpl(resTable, context_);
    }

    @Override
    public Vector<String> take(int num) throws Exception {
        Iterator<Row> rowIter = Master.kvs.scan(tableName_, null, null);

        Vector<String> toReturn = new Vector<>();
        while (rowIter.hasNext() && num-- != 0) {
            toReturn.add(rowIter.next().get("value"));
        }

        return toReturn;
    }

    @Override
    public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
        String resTable = context_.invokeOperation("/rdd/fold",
                Serializer.objectToByteArray(lambda), new String(tableName_), zeroElement);
        String finalAggregationTable = context_.invokeOperation("/pairRdd/foldByKey",
                Serializer.objectToByteArray(lambda), resTable, zeroElement);
        Iterator<Row> res = Master.kvs.scan(finalAggregationTable);
        return res.next().get("value");
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        String resTable = context_.invokeOperation("/rdd/flatMapToPair", Serializer.objectToByteArray(lambda),
                new String(tableName_), null);
        return new FlamePairRDDImpl(resTable, context_);
    }

    // ----------------------- EXTRA CREDIT ITEMS ---------------------------

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'intersection'");
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'sample'");
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'groupBy'");
    }

    @Override
    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'filter'");
    }

    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'mapPartitions'");
    }
}
