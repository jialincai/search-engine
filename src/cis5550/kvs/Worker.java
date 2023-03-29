package cis5550.kvs;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

import cis5550.tools.Logger;
import static cis5550.webserver.Server.*;

public class Worker extends cis5550.generic.Worker {
    protected static Map<String, Map<String, Row>> tables_ = Collections.synchronizedMap(new HashMap<>());
    protected static Map<String, RandomAccessFile> persistantTables_ = Collections.synchronizedMap(new HashMap<>());

    // In-memory index that maps row keys to the file offset.
    protected static Map<String, Integer> diskOffset_ = Collections.synchronizedMap(new HashMap<>());

    public static void main(String[] args) throws IOException {
        ip_ = InetAddress.getLocalHost().getHostAddress();
        try {
            port(Integer.parseInt(args[0]));
            storageDirectory_ = new String(args[1]);
            String[] masterInfo = args[2].split(":");
            masterIp_ = masterInfo[0];
            masterPortNumber_ = Integer.parseInt(masterInfo[1]);
        } catch (NumberFormatException e) {
            Logger.getLogger(Worker.class).fatal("Unable to parse port number.", e);
            throw e;
        } catch (ArrayIndexOutOfBoundsException e) {
            Logger.getLogger(Worker.class).fatal("Invalid number of arguments.", e);
            throw e;
        }

        Path idPath = FileSystems.getDefault().getPath(storageDirectory_, "id");
        try {
            if (Files.notExists(idPath)) { // File created
                BufferedWriter out = new BufferedWriter(new FileWriter(idPath.toString()));
                id_ = randomWorkerId();
                out.write(id_);
                out.close();
            } else { // File already exists
                BufferedReader in = new BufferedReader(new FileReader(idPath.toString()));
                String id = in.readLine();
                id_ = id;
            }
        } catch (IOException e) {
            Logger.getLogger(Worker.class).warn("Worker ID could not be read/written to file.", e);
            throw e;
        }

        reconstructIndex();

        put("/data/:T/:R/:C", (req, res) -> {
            synchronized (tables_) {
                String tableKey = req.params("T");
                String rowKey = req.params("R");
                String colomnKey = req.params("C");

                String ifColumn = req.queryParams("ifColumn");
                String equals = req.queryParams("equals");

                boolean isCondPut = ifColumn != null && equals != null ? true : false;
                boolean isPersistant = persistantTables_.get(tableKey) != null ? true : false;

                Map<String, Row> table;
                if (tableKey != null && rowKey != null && colomnKey != null) {
                    table = tables_.get(tableKey);
                    if (table == null) {
                        table = Collections.synchronizedMap(new HashMap<>());
                        tables_.put(tableKey, table);
                    }

                    Row row = table.get(rowKey);
                    if (row == null) {
                        row = new Row(rowKey);
                        table.put(rowKey, row);
                    }

                    if (isCondPut) {
                        String value = row.get(ifColumn);
                        if (value == null || !value.equals(equals)) {
                            return "Fail";
                        }
                    }

                    if (isPersistant) {
                        RandomAccessFile tableFile = persistantTables_.get(tableKey);
                        Row diskRow;
                        if (row.get("pos") == null) {
                            diskRow = new Row(rowKey);
                            diskRow.put(colomnKey, req.bodyAsBytes());

                            row.put("pos", Long.toString(tableFile.length()));
                            tableFile.seek(tableFile.length());
                            tableFile.write(diskRow.toByteArray());
                            tableFile.write('\n');
                        } else {
                            long rowOffset = Long.parseLong(row.get("pos"));
                            tableFile.seek(rowOffset);
                            diskRow = Row.readFrom(tableFile);
                            diskRow.put(colomnKey, req.bodyAsBytes());

                            row.put("pos", Long.toString(tableFile.length()));
                            tableFile.seek(tableFile.length());
                            tableFile.write(diskRow.toByteArray());
                            tableFile.write('\n');
                        }
                    } else {
                        row.put(colomnKey, req.bodyAsBytes());
                    }

                    return "OK";
                }
                res.status(404, "Not Found");
                return "Not Found";
            }
        });

        get("/data/:T/:R/:C", (req, res) -> {
            synchronized (tables_) {
                String tableKey = req.params("T");
                String rowKey = req.params("R");
                String colomnKey = req.params("C");

                boolean isPersistant = persistantTables_.get(tableKey) != null ? true : false;

                try {
                    byte[] value;
                    if (isPersistant) {
                        RandomAccessFile tableFile = persistantTables_.get(tableKey);
                        long rowOffset = Long.parseLong(tables_.get(tableKey).get(rowKey).get("pos"));
                        tableFile.seek(rowOffset);
                        Row row = Row.readFrom(tableFile);
                        value = row.getBytes(colomnKey);
                    } else {
                        value = tables_.get(tableKey).get(rowKey).getBytes(colomnKey);
                    }

                    if (value == null) {
                        throw new Exception();
                    }
                    res.bodyAsBytes(value);
                    return null;
                } catch (Exception e) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }
            }
        });

        put("/persist/:XXX", (req, res) -> {
            String tableName = req.params("XXX");

            if (tables_.get(tableName) == null) {
                synchronized (tables_) {
                    synchronized (persistantTables_) {
                        Map<String, Row> newTable = Collections.synchronizedMap(new HashMap<>());
                        tables_.put(tableName, newTable);

                        File newFile = new File(String.format("%s/%s.table", storageDirectory_, tableName));
                        RandomAccessFile randAccessWrapper = new RandomAccessFile(newFile, "rwd");
                        persistantTables_.put(tableName, randAccessWrapper);

                        return "OK";
                    }
                }
            } else {
                res.status(403, "Forbidden Error");
                return "403 Forbidden Error -- Table exists already";
            }
        });

        get("/data/:XXX/:YYY", (req, res) -> {
            res.header("Content-Type", "text/plain");
            String tableKey = req.params("XXX");
            String rowKey = req.params("YYY");

            boolean isPersistant = persistantTables_.get(tableKey) != null ? true : false;

            if (tables_.get(tableKey) != null && tables_.get(tableKey).get(rowKey) != null) {
                Row row = tables_.get(tableKey).get(rowKey);

                if (isPersistant) {
                    RandomAccessFile tableFile = persistantTables_.get(tableKey);
                    long fileOffset = Long.parseLong(row.get("pos"));
                    tableFile.seek(fileOffset);
                    row = Row.readFrom(tableFile);
                }

                res.bodyAsBytes(row.toByteArray());
                return null;
            } else {
                res.status(404, "Not Found");
                return "Not Found";
            }
        });

        get("/data/:XXX", (req, res) -> {
            res.header("Content-Type", "text/plain");
            String tableKey = req.params("XXX");

            String startRow = req.queryParams("startRow");
            String endRowExclusive = req.queryParams("endRowExclusive");

            boolean isPersistant = persistantTables_.get(tableKey) != null ? true : false;

            Map<String, Row> table = tables_.get(tableKey);
            if (table != null) {
                byte[] linefeed = { 0x0A };
                if (isPersistant) {
                    RandomAccessFile tableFile = persistantTables_.get(tableKey);
                    for (Row indexRow : table.values()) {
                        if (checkWithinBounds(indexRow.key, startRow, endRowExclusive)) {
                            long fileOffset = Long.parseLong(indexRow.get("pos"));
                            tableFile.seek(fileOffset);
                            Row row = Row.readFrom(tableFile);
                            res.write(row.toByteArray());
                            res.write(linefeed);
                        }
                    }
                } else {
                    for (Row row : table.values()) {
                        if (checkWithinBounds(row.key, startRow, endRowExclusive)) {
                            res.write(row.toByteArray());
                            res.write(linefeed);
                        }
                    }
                }
                res.write(linefeed);
                return null;
            } else {
                res.status(404, "Not Found");
                return "Not Found";
            }
        });

        put("/data/:XXX", (req, res) -> {
            String tableKey = req.params("XXX");
            Map<String, Row> table = tables_.get(tableKey);

            if (table == null) {
                table = Collections.synchronizedMap(new HashMap<>());
                tables_.put(tableKey, table);
            }

            boolean isPersistant = persistantTables_.get(tableKey) != null ? true : false;

            InputStream bodyStream = new ByteArrayInputStream(req.bodyAsBytes());
            Row newRow = Row.readFrom(bodyStream);

            while (newRow != null) {
                if (isPersistant) {
                    RandomAccessFile tableFile = persistantTables_.get(tableKey);
                    Row newIndexRow = new Row(newRow.key);
                    newIndexRow.put("pos", Long.toString(tableFile.length()));
                    table.put(newRow.key, newIndexRow);
                    tableFile.write(newRow.toByteArray());
                } else {
                    table.put(newRow.key, newRow);
                }
                newRow = Row.readFrom(bodyStream);
            }
            return "OK";
        });

        put("/rename/:XXX", (req, res) -> {
            String tableKey = req.params("XXX");
            String newTableName = req.body();

            boolean isPersistant = persistantTables_.get(tableKey) != null ? true : false;

            if (tables_.get(tableKey) != null) {
                if (tables_.get(newTableName) == null) {
                    Map<String, Row> table = tables_.get(tableKey);
                    tables_.put(newTableName, table);
                    tables_.remove(tableKey);

                    if (isPersistant) {
                        File orig = new File(String.format("%s/%s.table", storageDirectory_, tableKey));
                        File rename = new File(String.format("%s/%s.table", storageDirectory_, newTableName));
                        if (orig.renameTo(rename) == false) {
                            Logger.getLogger(Worker.class)
                                    .warn(String.format("Failed to rename %s.table %s.", tableKey));
                        }
                    }

                    return "OK";
                } else {
                    res.status(409, "Conflict");
                    return "Conflict";
                }
            } else {
                res.status(404, "Not Found");
                return "Not Found";
            }
        });

        put("/delete/:XXX", (req, res) -> {
            String tableKey = req.params("XXX");

            if (tables_.get(tableKey) != null) {
                tables_.remove(tableKey);
                File file = new File(String.format("%s/%s.table", storageDirectory_, tableKey));
                file.delete();
                return "OK";
            } else {
                res.status(404, "Not Found");
                return "Not Found";
            }
        });

        get("/tables", (req, res) -> {
            res.header("Content-Type", "text/plain");

            String toReturn = "";
            for (String tableKey : tables_.keySet()) {
                toReturn += tableKey + "\n";
            }

            return toReturn;
        });

        get("/count/:XXX", (req, res) -> {
            String tableName = req.params("XXX");

            Map<String, Row> table = tables_.get(tableName);

            if (table != null) {
                return Long.toString(table.size());
            } else {
                res.status(404, "Not Found");
                return "Not Found";
            }
        });

        get("/", (req, res) -> {
            res.header("Content-Type", "text/html");
            return tablesPage();
        });

        get("/view/:XXX", (req, res) -> {
            String tableName = req.params("XXX");

            Map<String, Row> table = tables_.get(tableName);
            if (table != null) {
                res.header("Content-Type", "text/html");
                return tablePage(tableName, table);
            } else {
                res.status(404, "Not Found");
            }

            // return HTML page with 10 rows of data
            // one HTML row = one row of data
            // columns: rowKey, each column name that appears at least once in those 10 rows

            // The cells should contain the values in the relevant rows and columns
            // empty if the row does not contain a column with that name

            // Sort by rowKey (Priority1), columnKey

            // If table contains 10+ rows:
            // route should display the first 10
            // “Next” link at the end of the table that displays another table with the next
            // 10 rows.

            // You may add query parameters to the route for this purpose.

            return null;
        });

        startPingThread();
    }

    private synchronized static void reconstructIndex() {
        File storageDirectory = new File(storageDirectory_);
        File[] files = storageDirectory.listFiles();
        for (File file : files) {
            String fileName = file.getName();
            if (fileName.endsWith(".table")) {
                String tableKey = fileName.substring(0, fileName.length() - ".table".length());
                Map<String, Row> newTable = Collections.synchronizedMap(new HashMap<>());
                tables_.put(tableKey, newTable);

                try {
                    RandomAccessFile tableFile = new RandomAccessFile(file, "rwd");
                    persistantTables_.put(tableKey, tableFile);
                    long fileOffset = 0;

                    Row deserializedRow = Row.readFrom(tableFile);
                    while (deserializedRow != null) {
                        String rowKey = deserializedRow.key;
                        Row rowInNewTable = newTable.get(rowKey);
                        if (rowInNewTable == null) {
                            rowInNewTable = new Row(rowKey);
                            rowInNewTable.put("pos", Long.toString(fileOffset));
                            newTable.put(rowKey, rowInNewTable);
                        } else {
                            rowInNewTable.put("pos", Long.toString(fileOffset));
                        }
                        fileOffset = tableFile.getFilePointer();
                        deserializedRow = Row.readFrom(tableFile);
                    }
                } catch (FileNotFoundException e) {
                    Logger.getLogger(Worker.class).warn(String.format("Unable to open file %s.", file.getPath()), e);
                } catch (Exception e) {
                    Logger.getLogger(Worker.class).warn("Unable to deserialize row.", e);
                }
            }
        }
    }

    private static String tablesPage() throws IOException {
        // Read tables.html into a String.
        StringBuilder contentBuilder = new StringBuilder();
        BufferedReader in = new BufferedReader(new FileReader("src/cis5550/html/tables.html"));
        String str;
        while ((str = in.readLine()) != null) {
            contentBuilder.append(str);
        }
        in.close();
        String toReturn = contentBuilder.toString();

        // Html for all table information.
        contentBuilder.setLength(0);
        synchronized (tables_) {
            for (var table : tables_.entrySet()) {
                String tableName = table.getKey();
                String link = String.format("http://%s:%d/view/%s", ip_, portNumber_, tableName);
                int rowCount = table.getValue().size();
                String isPersistant = persistantTables_.get(tableName) != null ? "persistant" : "";
                contentBuilder.append("<tr>")
                        .append(String.format("<td>%s</td>", tableName))
                        .append(String.format("<td><a href=\"%s\">%s</td>", link, link))
                        .append(String.format("<td>%d</td>", rowCount))
                        .append(String.format("<td>%s</td>", isPersistant))
                        .append("</tr>");
            }
        }
        toReturn = toReturn.replace("<!-- INSERT_TABLE_HERE -->", contentBuilder.toString());
        return toReturn;
    }

    private static String tablePage(String tableKey, Map<String, Row> table) throws IOException {
        Set<String> rowKeys = new TreeSet<>();
        Set<String> columnKeys = new TreeSet<>();
        synchronized (table) {
            for (Row row : table.values()) {
                rowKeys.add(row.key);
                columnKeys.addAll(row.columns());
            }
        }

        // Read table.html into a String.
        StringBuilder contentBuilder = new StringBuilder();
        BufferedReader in = new BufferedReader(new FileReader("src/cis5550/html/table.html"));
        String str;
        while ((str = in.readLine()) != null) {
            contentBuilder.append(str);
        }
        in.close();
        String toReturn = contentBuilder.toString();

        // Html for the table key header.
        toReturn = toReturn.replace("<!-- INSERT_TABLE_KEY_HERE -->", tableKey);

        // Html for the headers of the table.
        contentBuilder.setLength(0);
        for (String columnKey : columnKeys) {
            contentBuilder.append(String.format("<th>%s</th>", columnKey));
        }
        toReturn = toReturn.replace("<!-- INSERT_COLUMN_KEYS_HERE -->", contentBuilder.toString());

        // Html for a single table's information.
        contentBuilder.setLength(0);
        for (String rowKey : rowKeys) {
            contentBuilder.append("<tr>");
            contentBuilder.append(String.format("<td>%s</td>", rowKey));
            for (String columnKey : columnKeys) {
                String value = table.get(rowKey).get(columnKey);
                if (value != null) {
                    contentBuilder.append(String.format("<td>%s</td>", value));
                } else {
                    contentBuilder.append("<td></td>");
                }
            }
            contentBuilder.append("</tr>");
        }
        toReturn = toReturn.replace("<!-- INSERT_TABLE_HERE -->", contentBuilder.toString());

        return toReturn;
    }

    public static boolean checkWithinBounds(String toCompare, String inclusiveStart, String exclusiveEnd) {
        if (inclusiveStart == null && exclusiveEnd == null) {
            return true;
        } else if (inclusiveStart == null) {
            return toCompare.compareTo(exclusiveEnd) < 0 ? true : false;
        } else if (exclusiveEnd == null) {
            return toCompare.compareTo(inclusiveStart) >= 0 ? true : false;
        } else {
            return toCompare.compareTo(inclusiveStart) >= 0 && toCompare.compareTo(exclusiveEnd) < 0 ? true : false;
        }
    }
}
