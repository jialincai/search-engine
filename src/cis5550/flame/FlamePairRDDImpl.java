package cis5550.flame;

import java.util.*;

import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD {
    String tableName_;
    FlameContextImpl context_;

    public FlamePairRDDImpl(String tableName, FlameContextImpl context) {
        tableName_ = tableName;
        context_ = context;
    }

    @Override
    public List<FlamePair> collect() throws Exception {
        List<FlamePair> allValues = new LinkedList<>();
        Iterator<Row> iter = Master.kvs.scan(tableName_);

        while (iter.hasNext()) {
            Row row = iter.next();
            for (String column : row.columns()) {
                allValues.add(new FlamePair(row.key(), row.get(column)));
            }
        }
        return allValues;
    }

    @Override
    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        String resTable = context_.invokeOperation("/pairRdd/foldByKey", Serializer.objectToByteArray(lambda),
                new String(tableName_), zeroElement);
        return new FlamePairRDDImpl(resTable, context_);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        Master.kvs.rename(tableName_, tableNameArg);
        tableName_ = tableNameArg;
    }

    @Override
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        String resTable = context_.invokeOperation("/pairRdd/flatMap", Serializer.objectToByteArray(lambda),
                new String(tableName_), null);
        return new FlameRDDImpl(resTable, context_);
    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        String resTable = context_.invokeOperation("/pairRdd/flatMapToPair",
                Serializer.objectToByteArray(lambda),
                new String(tableName_), null);
        return new FlamePairRDDImpl(resTable, context_);
    }

    @Override
    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        String resTable = context_.invokeOperation("/pairRdd/join", null,
                new String(tableName_), ((FlamePairRDDImpl) other).tableName_);
        return new FlamePairRDDImpl(resTable, context_);
    }

    @Override
    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'cogroup'");
    }

}
