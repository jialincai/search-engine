package cis5550.flame;

import java.util.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.io.*;
import java.io.ObjectInputFilter.Status;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {

  public static void main(String args[]) {
    if (args.length != 2) {
      System.err.println("Syntax: Worker <port> <masterIP:port>");
      System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
    startPingThread(server, "" + port, port);
    final File myJAR = new File("__worker" + port + "-current.jar");

    port(port);

    post("/useJAR", (request, response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });

    post("/rdd/distinct", (request, response) -> {
      Map<String, String> query = decodeURL(request, false);
      byte[] lambda_raw = request.bodyAsBytes();

      if (query == null) {
        response.status(422, "Unprocessable Entity");
        return "Unprocessable Entity";
      }

      StringToIterable lambda = (StringToIterable) Serializer.byteArrayToObject(lambda_raw, myJAR);
      KVSClient kvs = new KVSClient(query.get("kvsMaster"));
      Iterator<Row> kvs_rows = kvs.scan(query.get("in"), query.get("start"), query.get("end"));

      while (kvs_rows.hasNext()) {
        Row row = kvs_rows.next();
        kvs.put(query.get("out"), row.get("value"), "value", row.get("value"));
      }
      return "OK";
    });

    post("/rdd/flatMap", (request, response) -> {
      Map<String, String> query = decodeURL(request, false);
      byte[] lambda_raw = request.bodyAsBytes();

      if (query == null) {
        response.status(422, "Unprocessable Entity");
        return "Unprocessable Entity";
      }

      StringToIterable lambda = (StringToIterable) Serializer.byteArrayToObject(lambda_raw, myJAR);
      KVSClient kvs = new KVSClient(query.get("kvsMaster"));
      Iterator<Row> kvs_rows = kvs.scan(query.get("in"), query.get("start"), query.get("end"));

      while (kvs_rows.hasNext()) {
        Row row = kvs_rows.next();
        Iterable<String> lambda_out = lambda.op(row.get("value"));
        if (lambda_out == null) {
          continue;
        }
        int seqNo = 0;
        for (String entry : lambda_out) {
          kvs.put(query.get("out"), row.key() + ":" + seqNo++, "value", entry);
        }
      }
      return "OK";
    });

    post("/rdd/flatMapToPair", (request, response) -> {
      Map<String, String> query = decodeURL(request, false);
      byte[] lambda_raw = request.bodyAsBytes();

      if (query == null) {
        response.status(422, "Unprocessable Entity");
        return "Unprocessable Entity";
      }

      StringToPairIterable lambda = (StringToPairIterable) Serializer.byteArrayToObject(lambda_raw, myJAR);
      KVSClient kvs = new KVSClient(query.get("kvsMaster"));
      Iterator<Row> kvs_rows = kvs.scan(query.get("in"), query.get("start"), query.get("end"));

      while (kvs_rows.hasNext()) {
        Row row = kvs_rows.next();
        Iterable<FlamePair> lambda_out = lambda.op(row.get("value"));
        if (lambda_out == null) {
          continue;
        }

        for (FlamePair pair : lambda_out) {
          // TODO : ask if multiple FlamePairs can have the same key. From the same
          // invocation of lambda.
          kvs.put(query.get("out"), pair._1(), row.key(), pair._2());
        }
      }
      return "OK";
    });

    post("/rdd/mapToPair", (request, response) -> {
      Map<String, String> query = decodeURL(request, false);
      byte[] lambda_raw = request.bodyAsBytes();

      if (query == null) {
        response.status(422, "Unprocessable Entity");
        return "Unprocessable Entity";
      }

      StringToPair lambda = (StringToPair) Serializer.byteArrayToObject(lambda_raw, myJAR);
      KVSClient kvs = new KVSClient(query.get("kvsMaster"));
      Iterator<Row> kvs_rows = kvs.scan(query.get("in"), query.get("start"), query.get("end"));

      while (kvs_rows.hasNext()) {
        Row row = kvs_rows.next();
        FlamePair lambda_out = lambda.op(row.get("value"));
        if (lambda_out == null) {
          continue;
        }
        kvs.put(query.get("out"), lambda_out._1(), row.key(), lambda_out._2());
      }

      return "OK";
    });

    post("/pairRdd/foldByKey", (request, response) -> {
      Map<String, String> query = decodeURL(request, true);
      byte[] lambda_raw = request.bodyAsBytes();

      if (query == null) {
        response.status(422, "Unprocessable Entity");
        return "Unprocessable Entity";
      }

      TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(lambda_raw, myJAR);
      KVSClient kvs = new KVSClient(query.get("kvsMaster"));
      Iterator<Row> kvs_rows = kvs.scan(query.get("in"), query.get("start"), query.get("end"));

      while (kvs_rows.hasNext()) {
        Row row = kvs_rows.next();
        String accumulator = query.get("arg");
        for (String column : row.columns()) {
          accumulator = lambda.op(accumulator, row.get(column));
        }
        kvs.put(query.get("out"), row.key(), "value", accumulator);
      }

      return "OK";
    });

    post("/rdd/fold", (request, response) -> {
      Map<String, String> query = decodeURL(request, true);
      byte[] lambda_raw = request.bodyAsBytes();

      if (query == null) {
        response.status(422, "Unprocessable Entity");
        return "Unprocessable Entity";
      }

      TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(lambda_raw, myJAR);
      KVSClient kvs = new KVSClient(query.get("kvsMaster"));
      Iterator<Row> kvs_rows = kvs.scan(query.get("in"), query.get("start"), query.get("end"));

      String accumulator = query.get("arg");
      while (kvs_rows.hasNext()) {
        Row row = kvs_rows.next();
        accumulator = lambda.op(accumulator, row.get("value"));
      }
      kvs.put(query.get("out"), query.get("in"), query.get("start") + query.get("end"), accumulator);

      return "OK";
    });

    post("/pairRdd/flatMap", (request, response) -> {
      Map<String, String> query = decodeURL(request, false);
      byte[] lambda_raw = request.bodyAsBytes();

      if (query == null) {
        response.status(422, "Unprocessable Entity");
        return "Unprocessable Entity";
      }

      PairToStringIterable lambda = (PairToStringIterable) Serializer.byteArrayToObject(lambda_raw, myJAR);
      KVSClient kvs = new KVSClient(query.get("kvsMaster"));
      Iterator<Row> kvs_rows = kvs.scan(query.get("in"), query.get("start"), query.get("end"));

      while (kvs_rows.hasNext()) {
        Row row = kvs_rows.next();
        for (String columnKey : row.columns()) {
          Iterable<String> lambda_out = lambda.op(new FlamePair(row.key(), row.get(columnKey)));
          if (lambda_out == null) {
            continue;
          }

          int seqNo = 0;
          for (String value : lambda_out) {
            kvs.put(query.get("out"), String.format("%s:%s:%s", row.key(), columnKey, seqNo++), "value", value);
          }
        }
      }

      return "OK";
    });

    post("/pairRdd/flatMapToPair", (request, response) -> {
      Map<String, String> query = decodeURL(request, false);
      byte[] lambda_raw = request.bodyAsBytes();

      if (query == null) {
        response.status(422, "Unprocessable Entity");
        return "Unprocessable Entity";
      }

      PairToPairIterable lambda = (PairToPairIterable) Serializer.byteArrayToObject(lambda_raw, myJAR);
      KVSClient kvs = new KVSClient(query.get("kvsMaster"));
      Iterator<Row> kvs_rows = kvs.scan(query.get("in"), query.get("start"), query.get("end"));

      while (kvs_rows.hasNext()) {
        Row row = kvs_rows.next();
        for (String columnKey : row.columns()) {
          Iterable<FlamePair> lambda_out = lambda.op(new FlamePair(row.key(), row.get(columnKey)));
          if (lambda_out == null) {
            continue;
          }

          for (FlamePair pair : lambda_out) {
            // TODO : ask if multiple FlamePairs can have the same key. From the same
            // invocation of lambda.
            kvs.put(query.get("out"), pair._1(), String.format("%s:%s", row.key(), columnKey), pair._2());
          }
        }
      }

      return "OK";
    });

    post("/context/fromTable", (request, response) -> {
      Map<String, String> query = decodeURL(request, false);
      byte[] lambda_raw = request.bodyAsBytes();

      if (query == null) {
        response.status(422, "Unprocessable Entity");
        return "Unprocessable Entity";
      }

      RowToString lambda = (RowToString) Serializer.byteArrayToObject(lambda_raw, myJAR);
      KVSClient kvs = new KVSClient(query.get("kvsMaster"));
      Iterator<Row> kvs_rows = kvs.scan(query.get("in"), query.get("start"), query.get("end"));

      while (kvs_rows.hasNext()) {
        Row row = kvs_rows.next();
        String res = lambda.op(row);
        if (res != null) {
          kvs.put(query.get("out"), row.key(), "value", res);
        }
      }

      return "OK";
    });

    post("/pairRdd/join", (request, response) -> {
      Map<String, String> query = decodeURL(request, true);

      if (query == null) {
        response.status(422, "Unprocessable Entity");
        return "Unprocessable Entity";
      }

      KVSClient kvs = new KVSClient(query.get("kvsMaster"));
      Iterator<Row> kvs_rows = kvs.scan(query.get("in"), query.get("start"), query.get("end"));

      while (kvs_rows.hasNext()) {
        Row row = kvs_rows.next();
        if (kvs.existsRow(query.get("arg"), row.key())) {
          Row toCompare = kvs.getRow(query.get("arg"), row.key());

          for (String columnKey : row.columns()) {
            for (String toCompareColumnKey : toCompare.columns()) {
              String value = new String(kvs.get(query.get("in"), row.key(), columnKey), StandardCharsets.UTF_8) + ","
                  + new String(kvs.get(query.get("arg"), row.key(), toCompareColumnKey), StandardCharsets.UTF_8);
              kvs.put(query.get("out"), row.key(), Hasher.hash(columnKey + "~" + toCompareColumnKey), value);
            }
          }
        }
      }

      return "OK";
    });
  }

  static Map<String, String> decodeURL(Request request, Boolean checkArg) {
    Map<String, String> toReturn = new HashMap<>();
    String start = request.queryParams("start");
    String end = request.queryParams("end");
    String in = request.queryParams("in");
    String out = request.queryParams("out");
    String kvsMaster = request.queryParams("kvs");

    if (in == null || out == null || kvsMaster == null) {
      return null;
    }

    if (checkArg) {
      String arg = request.queryParams("arg");
      if (arg == null) {
        return null;
      }
      toReturn.put("arg", arg);
    }

    toReturn.put("start", start);
    toReturn.put("end", end);
    toReturn.put("in", in);
    toReturn.put("out", out);
    toReturn.put("kvsMaster", kvsMaster);

    return toReturn;
  }
}
