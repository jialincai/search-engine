package cis5550.kvs;

import static cis5550.webserver.Server.get;

import java.io.*;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.webserver.*;
import cis5550.tools.*;
//import cis5550.webserver.Route;

import static cis5550.webserver.Server.*;

class Worker extends cis5550.generic.Worker {

	private static int SERVER_PORT = 8081;

	private static String STORAGE_DIRECTORY = "./__worker_new";

	private static String MASTER_ADDRESS = "localhost:8000";

	private static String PROTOCOL = "http";

	private static byte LINE_FEED = 0x0A;

	private static long LAST_ACTIVE = System.currentTimeMillis();

	private static List<String> WORKER_LIST = new ArrayList<String>();

	private static int MY_ID_RANK = 0;

	private static List<String> MY_NEIGHBOUR_WORKERS = new ArrayList<String>();

	private static Map<String, ConcurrentHashMap<String, Row>> KVStore = new HashMap<String, ConcurrentHashMap<String, Row>>(); // (table,
																																// row)
																																// ->
																																// Row

	private static Map<String, RandomAccessFile> persistentTableFile = new HashMap<String, RandomAccessFile>();

	private static Map<String, RandomAccessFile> nonPersistentTableFile = new HashMap<String, RandomAccessFile>();

	public static class GarbageCollector extends Thread {

		public void run() {
			while (true) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					System.out.println("Error at thread sleep in garbageCollector");
					e.printStackTrace();
				}

				if (System.currentTimeMillis() - LAST_ACTIVE >= 10000) {

					System.out.println("Collecting Garbage..");
					synchronized (KVStore) {
						for (Map.Entry<String, ConcurrentHashMap<String, Row>> KVEntry : KVStore.entrySet()) {
							String tableName = KVEntry.getKey();

							if (tableName.equals("index")) {
								RandomAccessFile oldTableFile = null;
								if (persistentTableFile.containsKey(tableName))
									oldTableFile = persistentTableFile.get(tableName);
								else
									oldTableFile = nonPersistentTableFile.get(tableName);

								if (oldTableFile != null) {
									synchronized (oldTableFile) {
										Path newPath = Paths.get(STORAGE_DIRECTORY, "~$" + tableName + ".table");
										Path oldPath = Paths.get(STORAGE_DIRECTORY, tableName + ".table");

										RandomAccessFile newTableFile = null;

										try {
											Files.deleteIfExists(newPath);
											Files.createFile(newPath);
											newTableFile = new RandomAccessFile(newPath.toString(), "rw");
										} catch (IOException e) {
											System.out.println("Error while creating a new file for persistent table.");
											e.printStackTrace();
										}

										for (ConcurrentHashMap.Entry<String, Row> row : KVEntry.getValue().entrySet()) {

											Row rowObjInMem = row.getValue();
											long fileLen = 0;
											try {
												fileLen = newTableFile.length();
											} catch (IOException e2) {
												e2.printStackTrace();
											}

											if (persistentTableFile.containsKey(tableName)) {

												Row rowObj = null;
												String seekPointerStr = rowObjInMem.get("pos");
												long seekPointer = 0;
												long fileLen_ = 0;

												try {
													fileLen_ = oldTableFile.length();
													seekPointer = Long.parseLong(seekPointerStr);
													oldTableFile.seek(seekPointer);
													rowObj = Row.readFrom(oldTableFile);

													synchronized (newTableFile) {
														newTableFile.seek(newTableFile.length());

														// System.out.println(rowObj.toString());
														newTableFile.write(rowObj.toByteArray());
														newTableFile.writeByte(LINE_FEED);
													}

												} catch (Exception e) {
													System.out.println(
															"Exception while reading row from table file in handling put new data route");
													e.printStackTrace();
												}

												// Update the seek pointer in KVStore
												rowObjInMem.put("pos", Long.toString(fileLen));

											} else {
												synchronized (newTableFile) {
													try {
														newTableFile.seek(fileLen);
														newTableFile.write(rowObjInMem.toByteArray());
														newTableFile.writeByte(LINE_FEED);
													} catch (IOException e) {
														e.printStackTrace();
													}

												}
											}
										}

										try {

											// newTableFile.close();
											// System.out.println("Debug table: "+tableName);
											Files.deleteIfExists(oldPath);
											Files.move(newPath, oldPath);
											if (persistentTableFile.containsKey(tableName)) {
												persistentTableFile.put(tableName, newTableFile);
											} else {
												nonPersistentTableFile.put(tableName, newTableFile);
											}

										} catch (IOException e) {
											e.printStackTrace();
										}

									}
								}
							}
						}
					}
				}
			}

		}
	}

	public static class updateWorkerList extends Thread {

		public void run() {

			try {
				while (true) {
					Thread.sleep(5000);

					URL url = new URL(PROTOCOL + "://" + MASTER_ADDRESS + "/workers");
					HttpURLConnection connection = (HttpURLConnection) url.openConnection();

					BufferedReader responseBR;
					if (100 <= connection.getResponseCode() && connection.getResponseCode() <= 399) {
						responseBR = new BufferedReader(new InputStreamReader(connection.getInputStream()));
					} else {
						responseBR = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
						break;
					}

					synchronized (MY_NEIGHBOUR_WORKERS) {

						WORKER_LIST = new ArrayList<String>();
						String worker;
						int numWorkers = Integer.parseInt(responseBR.readLine());
						while ((worker = responseBR.readLine()) != null) {
							WORKER_LIST.add(worker);
						}
						responseBR.close();
						if (numWorkers != WORKER_LIST.size())
							System.out.println("Worker list reading went wrong! ");

						Collections.sort(WORKER_LIST, new Comparator<String>() {
							@Override
							public int compare(String o1, String o2) {
								return o1.substring(0, o1.indexOf(',')).compareTo(o2.substring(0, o1.indexOf(',')));
							}
						});

						for (int i = 0; i < WORKER_LIST.size(); i++) {
							String _worker = WORKER_LIST.get(i);
							String workerID = _worker.substring(0, _worker.indexOf(','));
							if (workerID.equals(MY_ID))
								MY_ID_RANK = i;
						}

						if (numWorkers > 1) {
							MY_NEIGHBOUR_WORKERS = new ArrayList<String>();
							int n1 = (MY_ID_RANK + 1) % numWorkers;
							int n2 = (MY_ID_RANK + 2) % numWorkers;
							String wN1 = WORKER_LIST.get(n1);
							MY_NEIGHBOUR_WORKERS.add(wN1.substring(wN1.indexOf(',') + 1));
							if (n2 != MY_ID_RANK && n1 != n2) {
								String wN2 = WORKER_LIST.get(n2);
								MY_NEIGHBOUR_WORKERS.add(wN2.substring(wN2.indexOf(',') + 1));
							}

						}
					}

					// System.out.println("WORKER_LIST: "+WORKER_LIST);

				}

			} catch (MalformedURLException e) {
				System.out.println("MalformedURLException while updating workerList");
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("IOException while updating workerList");
				e.printStackTrace();
			} catch (InterruptedException e) {
				System.out.println("Error at thread sleep in updateWorkerList");
				e.printStackTrace();
			}
		}
	}

	public static class replicationMaintenance extends Thread {

		public void run() {
			try {
				while (true) {
					Thread.sleep(30000);

					URL url = new URL(PROTOCOL + "://" + MASTER_ADDRESS + "/workers");
					HttpURLConnection connection = (HttpURLConnection) url.openConnection();

					BufferedReader responseBR;
					if (100 <= connection.getResponseCode() && connection.getResponseCode() <= 399) {
						responseBR = new BufferedReader(new InputStreamReader(connection.getInputStream()));
					} else {
						responseBR = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
					}

				}
			} catch (MalformedURLException e) {
				System.out.println("MalformedURLException in replicationMaintenance");
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("IOException in replicationMaintenance");
				e.printStackTrace();
			} catch (InterruptedException e) {
				System.out.println("Error at thread sleep in replicationMaintenance");
				e.printStackTrace();
			}
		}
	}

	private static void recoverTables() {

		File storageDirectory = new File(STORAGE_DIRECTORY);

		File[] files = storageDirectory.listFiles();

		for (File file : files) {
			if (file.isFile() && file.getName().endsWith(".table")) {

				String tableFileName = file.getName();

				System.out.println("Recovering table " + tableFileName);
				FileInputStream tableFileIn = null;

				String tableName = tableFileName.substring(0, tableFileName.indexOf(".table"));

				if (!KVStore.containsKey(tableName)) {

					ConcurrentHashMap<String, Row> table = new ConcurrentHashMap<String, Row>();
					KVStore.put(tableName, table);

					Path path = Paths.get(STORAGE_DIRECTORY, tableFileName);
					RandomAccessFile tableFile;

					try {
						tableFile = new RandomAccessFile(path.toString(), "rw");
						persistentTableFile.put(tableName, tableFile);
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					}

				}

				try {
					tableFileIn = new FileInputStream(file);
				} catch (FileNotFoundException e) {
					System.out.println("FileNotFoundException while reading the table file during server recovery");
					e.printStackTrace();
				}

				long time1 = System.currentTimeMillis();
				Row rowObj;
				try {
					long seekPointer = 0;
					while ((rowObj = Row.readFrom(tableFileIn)) != null) {

						String row = rowObj.key;

						// System.out.println("Recovering row: " + row);

						Row rowObjInMem = null;
						if (!KVStore.get(tableName).containsKey(row)) {
							rowObjInMem = new Row(row);
						} else {
							rowObjInMem = KVStore.get(tableName).get(row);
						}
						rowObjInMem.put("pos", Long.toString(seekPointer));

						KVStore.get(tableName).put(row, rowObjInMem);
						seekPointer = tableFileIn.getChannel().position();

					}
				} catch (Exception e) {
					System.out.println("Exception occured while reading row from table file " + file.getName()
							+ " during recovery");
					e.printStackTrace();
				}

				long time2 = System.currentTimeMillis();
				System.out.println("Recovery took: " + (time2 - time1) / 1000);

			}
		}

	}

	private static String getBasicTableInfo(Request req, Response res) {

		res.type("text/html");

		String htmlTablesInfo = "";
		int curTable = 0;

		for (Map.Entry<String, ConcurrentHashMap<String, Row>> table : KVStore.entrySet()) {

			int numKeys = table.getValue().size();
			curTable += 1;
			String isPersistent = "";
			if (persistentTableFile.containsKey(table.getKey()))
				isPersistent = "persistent";

			htmlTablesInfo += "<tr>"
					+ String.format("<td> %d </td>", curTable)
					+ String.format("<td><a href=\"/view/%s\"> %s </a></td>", table.getKey(), table.getKey())
					+ String.format("<td> %s </td>", numKeys)
					+ String.format("<td> %s </td>", isPersistent)
					+ "</tr>";
		}

		if (htmlTablesInfo == "") {
			htmlTablesInfo = "No Tables in KVStore to display.";
		} else {
			htmlTablesInfo = "<table>"
					+ "<thead><tr>"
					+ "		<th>S.No</th>"
					+ "		<th>Table Name</th>"
					+ "		<th>Number of Keys</th>"
					+ "		<th>Persistent</th>"
					+ "	</tr></thead><tbody>"
					+ htmlTablesInfo + "</tbody></table>";
		}

		return "<!DOCTYPE html><html>"
				+ "<head> KVS Store Table "
				+ "<style>"
				+ "		table {"
				+ "			border-collapse: collapse;"
				+ "			width: 100%;\n"
				+ "		}\n"
				+ "		\n"
				+ "		th, td {\n"
				+ "			text-align: left;\n"
				+ "			padding: 8px;\n"
				+ "		}\n"
				+ "		\n"
				+ "		tr:nth-child(even) {\n"
				+ "			background-color: #f2f2f2;\n"
				+ "		}\n"
				+ "		\n"
				+ "		th {\n"
				+ "			background-color: #4CAF50;\n"
				+ "			color: white;\n"
				+ "		}\n"
				+ "		\n"
				+ "		a {\n"
				+ "			text-decoration: underline;\n"
				+ "			color: #4CAF50;\n"
				+ "		}\n"
				+ "	</style></head><body>" + htmlTablesInfo + "</body></html>";

	}

	private static String getHTMLFormattedTable(ConcurrentHashMap<String, Row> table, String tableName,
			List<String> rowList, List<String> columnList, int curRow, int nextFromRow) {

		String htmlRowsInfo = "";

		for (int i = 0; i < rowList.size(); i++) {
			Row row = table.get(rowList.get(i));

			htmlRowsInfo += "<tr>"
					+ String.format("<td> %d </td>", curRow)
					+ String.format("<td> %s </td>", rowList.get(i));

			for (int j = 0; j < columnList.size(); j++) {

				String columnValue = null;

				if (persistentTableFile.containsKey(tableName)) {

					RandomAccessFile tableFile = persistentTableFile.get(tableName);

					synchronized (tableFile) {
						String seekPointerStr = row.get("pos");

						long seekPointer = Long.parseLong(seekPointerStr);
						Row rowObj = null;

						try {
							tableFile.seek(seekPointer);
							rowObj = Row.readFrom(tableFile);
						} catch (IOException e1) {
							System.out.println("IOException while seeking table file to a pointer position");
							e1.printStackTrace();
						} catch (Exception e) {
							System.out.println(
									"Exception while reading row from table file in handling put new data route");
							e.printStackTrace();
						}
						columnValue = rowObj.get(columnList.get(j));
					}

				} else {

					columnValue = row.get(columnList.get(j));
				}

				if (columnValue != null) {
					htmlRowsInfo += String.format("<td> %s </td>", columnValue);
				} else {
					htmlRowsInfo += "<td>  </td>";
				}
			}
			htmlRowsInfo += "</tr>";
			curRow += 1;
		}

		String columnNamesHTML = "";

		for (int i = 0; i < columnList.size(); i++) {
			columnNamesHTML += "<th>" + columnList.get(i) + "</th>";
		}

		htmlRowsInfo = "<table>"
				+ "<thead><tr>"
				+ "<th>S.No</th>"
				+ "<th>Row Name</th>"
				+ columnNamesHTML
				+ "	</tr></thead><tbody>"
				+ htmlRowsInfo + "</tbody></table>";

		String nextLink = "";

		if (nextFromRow != -1)
			nextLink = "<a href=\"/view/" + tableName + "?fromRow=" + Integer.toString(nextFromRow) + "\">Next</a>";
		else
			nextLink = "End Of Table";

		return "<!DOCTYPE html><html>"
				+ "<head> Content in KVS Store's Table " + tableName
				+ "<style>"
				+ "		table {"
				+ "			border-collapse: collapse;"
				+ "			width: 100%;\n"
				+ "		}\n"
				+ "		\n"
				+ "		th, td {\n"
				+ "			text-align: left;\n"
				+ "			padding: 8px;\n"
				+ "		}\n"
				+ "		\n"
				+ "		tr:nth-child(even) {\n"
				+ "			background-color: #f2f2f2;\n"
				+ "		}\n"
				+ "		\n"
				+ "		th {\n"
				+ "			background-color: #4CAF50;\n"
				+ "			color: white;\n"
				+ "		}\n"
				+ "		\n"
				+ "		a {\n"
				+ "			text-decoration: underline;\n"
				+ "			color: #4CAF50;\n"
				+ "		}\n"
				+ "	</style></head><body>" + htmlRowsInfo + "</body> <br> <br> <br>"
				+ nextLink + "</html>";
	}

	private static String getTableContent(Request req, Response res) {

		res.type("text/html");

		int fromRow = 0;
		if (req.queryParams("fromRow") != null)
			fromRow = Integer.parseInt(req.queryParams("fromRow"));

		String tableName = req.params("T");

		if (!KVStore.containsKey(tableName)) {
			res.status(404, "Error - Requested table not found");
			return "404 Error - Requested table not found";
		}

		ConcurrentHashMap<String, Row> table = KVStore.get(tableName);

		List<String> rowNames = new ArrayList<>(table.keySet());
		Collections.sort(rowNames);

		int numRows = rowNames.size();
		int currentRow = 0;

		while (currentRow < numRows) {

			if (currentRow < fromRow) {
				currentRow += 1;
				continue;
			}

			int rowsPerPage = 0;
			Set<String> columnSet = new HashSet<String>();
			List<String> rowList = new ArrayList<String>();
			while (currentRow < numRows && rowsPerPage < 10) {
				rowsPerPage += 1;
				String row = rowNames.get(currentRow);
				rowList.add(row);

				if (persistentTableFile.containsKey(tableName)) {

					RandomAccessFile tableFile = persistentTableFile.get(tableName);

					synchronized (tableFile) {
						String seekPointerStr = KVStore.get(tableName).get(row).get("pos");

						if (seekPointerStr == null) {
							res.status(404, "Error - Requested row not found");
							return "404 Error - Requested row not found";
						}

						long seekPointer = Long.parseLong(seekPointerStr);

						Row rowObj = null;

						try {
							tableFile.seek(seekPointer);
							rowObj = Row.readFrom(tableFile);
						} catch (IOException e1) {
							System.out.println("IOException while seeking table file to a pointer position");
							e1.printStackTrace();
						} catch (Exception e) {
							System.out.println(
									"Exception while reading row from table file in handling put new data route");
							e.printStackTrace();
						}

						columnSet.addAll(rowObj.columns());
					}

				} else {

					columnSet.addAll(table.get(row).columns());
				}
				currentRow += 1;
			}

			List<String> columnList = new ArrayList<>(columnSet);
			Collections.sort(columnList);

			int nextFromRow = -1;
			if (currentRow < numRows)
				nextFromRow = currentRow;

			return getHTMLFormattedTable(table, tableName, rowList, columnList, currentRow - rowList.size() + 1,
					nextFromRow);

		}

		return null;
	}

	private static String handlePUT(String tableName, String row, String column, Request req, Response res)
			throws IOException {

		String ifcolumn = req.queryParams("ifcolumn");
		String equals = req.queryParams("equals");

		// int replicatedReq = 0;
		// if(req.queryParams("replicated") != null) {
		// System.out.println("Encountered a replicated PUT request");
		// replicatedReq = Integer.parseInt(req.queryParams("replicated"));
		// }

		if (ifcolumn != null && !ifcolumn.equals("") && equals != null && !equals.equals("")) {
			if (!KVStore.containsKey(tableName) || !KVStore.get(tableName).containsKey(row)) {
				return "FAIL";
			}
			String columnValue = KVStore.get(tableName).get(row).get(ifcolumn);
			if (!(columnValue != null && columnValue.equals(equals))) {
				return "FAIL";
			}
		}

		if (!KVStore.containsKey(tableName)) {
			ConcurrentHashMap<String, Row> table = new ConcurrentHashMap<String, Row>();
			KVStore.put(tableName, table);

			Path path = Paths.get(STORAGE_DIRECTORY, tableName + ".table");
			RandomAccessFile tableFile;

			try {
				Files.createFile(path);
				tableFile = new RandomAccessFile(path.toString(), "rw");
				nonPersistentTableFile.put(tableName, tableFile);
			} catch (IOException e) {
				System.out.println("Error while creating a new file for non-persistent table.");
				e.printStackTrace();
			}
		}

		if (!KVStore.get(tableName).containsKey(row)) {
			Row rowObj = new Row(row);
			KVStore.get(tableName).put(row, rowObj);
		}

		if (persistentTableFile.containsKey(tableName)) {

			RandomAccessFile tableFile = persistentTableFile.get(tableName);

			Row rowObjInMem = KVStore.get(tableName).get(row);
			Row rowObj = null;

			synchronized (tableFile) {
				String seekPointerStr = rowObjInMem.get("pos");

				// Get previous Row state

				long fileLen = tableFile.length();

				if (seekPointerStr != null) {
					try {
						long seekPointer = Long.parseLong(seekPointerStr);
						tableFile.seek(seekPointer);
						rowObj = Row.readFrom(tableFile);
					} catch (IOException e1) {
						System.out.println("IOException while seeking table file to a pointer position");
						e1.printStackTrace();
					} catch (Exception e) {
						System.out
								.println("Exception while reading row from table file in handling put new data route");
						e.printStackTrace();
					}
				} else {
					rowObj = new Row(row);
				}

				// Add new data to this row
				rowObj.put(column, req.bodyAsBytes());

				// Put it at the end of .table file
				synchronized (tableFile) {
					tableFile.seek(fileLen);
					tableFile.write(rowObj.toByteArray());
					tableFile.writeByte(LINE_FEED);
				}

				// Update the seek pointer in KVStore
				rowObjInMem.put("pos", Long.toString(fileLen));
			}

		} else {

			Row rowObj = KVStore.get(tableName).get(row);
			rowObj.put(column, req.bodyAsBytes());

			RandomAccessFile tableFile = nonPersistentTableFile.get(tableName);

			if (tableFile == null) {
				System.out.println("tableName which gave null in nonPersistentTableFile = " + tableName);
				Path path = Paths.get(STORAGE_DIRECTORY, tableName + ".table");
				Files.createFile(path);
				tableFile = new RandomAccessFile(path.toString(), "rw");
				nonPersistentTableFile.put(tableName, tableFile);
			}

			long fileLen = tableFile.length();

			synchronized (tableFile) {
				tableFile.seek(fileLen);
				tableFile.write(rowObj.toByteArray());
				tableFile.writeByte(LINE_FEED);
			}
		}

		// System.out.println(row.compareTo(MY_ID) >= 0);
		// System.out.println(row.compareTo(MY_NEIGHBOUR_WORKERS.get(0)) < 0);
		// && row.compareTo(MY_ID) >= 0 && row.compareTo(MY_NEIGHBOUR_WORKERS.get(0)) <
		// 0
		// if (replicatedReq == 0 && MY_NEIGHBOUR_WORKERS.size() > 0 ) {
		//
		// for (String neighbourWorker : MY_NEIGHBOUR_WORKERS) {
		// URL url = new URL(PROTOCOL + "://" + neighbourWorker + req.url() +
		// "?replicated=1");
		// System.out.println("Sending request " + url.toString());
		// HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		// connection.setRequestMethod("PUT");
		// connection.setRequestProperty("Content-Type", "text/plain");
		// connection.setDoOutput(true);
		// OutputStream conOut = connection.getOutputStream();
		// conOut.write(req.bodyAsBytes());
		// conOut.close();
		//
		// int status = connection.getResponseCode();
		//// System.out.println("Status Code received: "+status);
		// connection.disconnect();
		// }
		// }

		return "OK";
	}

	private static String handleBatchPUT(String tableName, String rowString, String columnString, Request req,
			Response res) throws IOException {

		String ifcolumn = req.queryParams("ifcolumn");
		String equals = req.queryParams("equals");

		// int replicatedReq = 0;
		// if(req.queryParams("replicated") != null) {
		// System.out.println("Encountered a replicated PUT request");
		// replicatedReq = Integer.parseInt(req.queryParams("replicated"));
		// }

		if (!KVStore.containsKey(tableName)) {
			ConcurrentHashMap<String, Row> table = new ConcurrentHashMap<String, Row>();
			KVStore.put(tableName, table);

			Path path = Paths.get(STORAGE_DIRECTORY, tableName + ".table");
			RandomAccessFile tableFile;

			try {
				Files.createFile(path);
				tableFile = new RandomAccessFile(path.toString(), "rw");
				nonPersistentTableFile.put(tableName, tableFile);
			} catch (IOException e) {
				System.out.println("Error while creating a new file for non-persistent table.");
				e.printStackTrace();
			}
		}

		String seperator = "~`~";
		String[] rowList = rowString.split(seperator);
		String[] colList = columnString.split(seperator);
		String[] valList = req.body().split(seperator);

		/*
		 * System.out.println("\n-------------------------------------");
		 */

		/*
		 * System.out.println("rowList.length="+rowList.length);
		 * System.out.println("colList.length="+colList.length);
		 * System.out.println("valList.length="+valList.length);
		 * System.out.println();
		 * for (int i=0; i<rowList.length ; i++) {
		 * if (i >= valList.length) {
		 * System.out.println("row="+rowList[i]+"  col="+colList[i]);
		 * //
		 * System.out.println("row="+rowList[i]+"  col="+colList[i]+"  val="+valList[i])
		 * ;
		 * }
		 * }
		 * 
		 * System.out.println("-------------------------------------\n");
		 * 
		 */

		if (persistentTableFile.containsKey(tableName)) {

			RandomAccessFile tableFile = persistentTableFile.get(tableName);

			for (int ptr = 0; ptr < rowList.length; ptr++) {

				String row = rowList[ptr];
				String column = colList[ptr];

				if (!KVStore.get(tableName).containsKey(row)) {
					Row rowObj = new Row(row);
					KVStore.get(tableName).put(row, rowObj);
				}

				Row rowObjInMem = KVStore.get(tableName).get(row);
				Row rowObj = null;

				synchronized (tableFile) {
					String seekPointerStr = rowObjInMem.get("pos");

					// Get previous Row state

					long fileLen = tableFile.length();

					if (seekPointerStr != null) {
						try {
							long seekPointer = Long.parseLong(seekPointerStr);
							tableFile.seek(seekPointer);
							rowObj = Row.readFrom(tableFile);
						} catch (IOException e1) {
							System.out.println("IOException while seeking table file to a pointer position");
							e1.printStackTrace();
						} catch (Exception e) {
							System.out.println(
									"Exception while reading row from table file in handling put new data route");
							e.printStackTrace();
						}
					} else {
						rowObj = new Row(row);
					}

					// Add new data to this row
					rowObj.put(column, valList[ptr]);

					// Put it at the end of .table file
					synchronized (tableFile) {
						tableFile.seek(fileLen);
						tableFile.write(rowObj.toByteArray());
						tableFile.writeByte(LINE_FEED);
					}

					// Update the seek pointer in KVStore
					rowObjInMem.put("pos", Long.toString(fileLen));
				}
			}

		} else {

			for (int ptr = 0; ptr < rowList.length; ptr++) {

				String row = rowList[ptr];
				String column = colList[ptr];

				if (!KVStore.get(tableName).containsKey(row)) {
					Row rowObj = new Row(row);
					KVStore.get(tableName).put(row, rowObj);
				}

				Row rowObj = KVStore.get(tableName).get(row);
				rowObj.put(column, valList[ptr]);

				RandomAccessFile tableFile = nonPersistentTableFile.get(tableName);

				if (tableFile == null) {
					System.out.println("tableName which gave null in nonPersistentTableFile = " + tableName);
					Path path = Paths.get(STORAGE_DIRECTORY, tableName + ".table");
					Files.createFile(path);
					tableFile = new RandomAccessFile(path.toString(), "rw");
					nonPersistentTableFile.put(tableName, tableFile);
				}

				long fileLen = tableFile.length();

				synchronized (tableFile) {
					tableFile.seek(fileLen);
					tableFile.write(rowObj.toByteArray());
					tableFile.writeByte(LINE_FEED);
				}
			}
		}

		// System.out.println(row.compareTo(MY_ID) >= 0);
		// System.out.println(row.compareTo(MY_NEIGHBOUR_WORKERS.get(0)) < 0);
		// && row.compareTo(MY_ID) >= 0 && row.compareTo(MY_NEIGHBOUR_WORKERS.get(0)) <
		// 0
		// if (replicatedReq == 0 && MY_NEIGHBOUR_WORKERS.size() > 0 ) {
		//
		// for (String neighbourWorker : MY_NEIGHBOUR_WORKERS) {
		// URL url = new URL(PROTOCOL + "://" + neighbourWorker + req.url() +
		// "?replicated=1");
		// System.out.println("Sending request " + url.toString());
		// HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		// connection.setRequestMethod("PUT");
		// connection.setRequestProperty("Content-Type", "text/plain");
		// connection.setDoOutput(true);
		// OutputStream conOut = connection.getOutputStream();
		// conOut.write(req.bodyAsBytes());
		// conOut.close();
		//
		// int status = connection.getResponseCode();
		//// System.out.println("Status Code received: "+status);
		// connection.disconnect();
		// }
		// }

		return "OK";
	}

	private static String handleGET(String tableName, String row, String column, Request req, Response res) {

		if (!KVStore.containsKey(tableName)) {
			res.status(404, "Error - Requested table not found");
			return "404 Error - Requested table not found";
		}

		if (!KVStore.get(tableName).containsKey(row)) {
			res.status(404, "Error - Requested row not found");
			return "404 Error - Requested row not found";
		}

		String content = null;

		if (persistentTableFile.containsKey(tableName)) {

			RandomAccessFile tableFile = persistentTableFile.get(tableName);
			Row rowObj = null;

			synchronized (tableFile) {
				String seekPointerStr = KVStore.get(tableName).get(row).get("pos");

				if (seekPointerStr == null) {
					res.status(404, "Error - Requested row not found");
					return "404 Error - Requested row not found";
				}

				long seekPointer = Long.parseLong(seekPointerStr);

				try {
					tableFile.seek(seekPointer);
					rowObj = Row.readFrom(tableFile);
				} catch (IOException e1) {
					System.out.println("IOException while seeking table file to a pointer position");
					e1.printStackTrace();
				} catch (Exception e) {
					System.out.println("Exception while reading row from table file in handling put new data route");
					e.printStackTrace();
				}
			}
			content = rowObj.get(column);
			if (content != null)
				res.bodyAsBytes(rowObj.getBytes(column));

		} else {

			content = KVStore.get(tableName).get(row).get(column);
			if (content != null)
				res.bodyAsBytes(KVStore.get(tableName).get(row).getBytes(column));
		}

		if (content == null) {
			res.status(404, "Error - Requested column not found");
			return "404 Error - Requested column not found";
		}

		return null;
	}

	private static String addPersistentTable(Request req, Response res) {

		String tableName = req.params("T");

		// int replicatedReq = 0;
		// if(req.queryParams("replicated") != null) {
		// System.out.println("Encountered a replicated persist request");
		// replicatedReq = Integer.parseInt(req.queryParams("replicated"));
		// }

		if (KVStore.containsKey(tableName)) {
			res.status(403, "Error Requested table name already exists");
			return "Choose an unused table name";
		}

		synchronized (persistentTableFile) {

			ConcurrentHashMap<String, Row> table = new ConcurrentHashMap<String, Row>();
			KVStore.put(tableName, table);

			Path path = Paths.get(STORAGE_DIRECTORY, tableName + ".table");
			RandomAccessFile tableFile;
			try {
				Files.createFile(path);
				tableFile = new RandomAccessFile(path.toString(), "rw");
				persistentTableFile.put(tableName, tableFile);
			} catch (IOException e) {
				System.out.println("Error while creating a new file for persistent table.");
				e.printStackTrace();
			}

		}

		// if (replicatedReq == 0 && WORKER_LIST.size() > 0 ) {
		// try {
		// for (String worker : WORKER_LIST) {
		// String workerID = worker.substring(0, worker.indexOf(','));
		// if (!workerID.equals(MY_ID)) {
		// String workerAddress = worker.substring(worker.indexOf(',') + 1);
		// URL url = new URL(PROTOCOL + "://" + workerAddress + req.url() +
		// "?replicated=1");
		//
		// System.out.println("Sending request " + url.toString());
		// HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		// connection.setRequestMethod("PUT");
		// connection.setDoOutput(true);
		// int status = connection.getResponseCode();
		// connection.disconnect();
		// }
		// }
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// }

		res.status(200, "OK");
		return "OK";

	}

	private static String wholeRowRead(Request req, Response res) {

		String tableName = req.params("T");
		String rowName = req.params("R");

		if (!KVStore.containsKey(tableName)) {
			res.status(404, "Error - Requested table not found");
			return "404 Error - Requested table not found";
		}

		if (!KVStore.get(tableName).containsKey(rowName)) {
			res.status(404, "Error - Requested row not found");
			return "404 Error - Requested row not found";
		}

		res.status(200, "OK");

		if (persistentTableFile.containsKey(tableName)) {

			RandomAccessFile tableFile = persistentTableFile.get(tableName);
			Row rowObj = null;

			synchronized (tableFile) {
				String seekPointerStr = KVStore.get(tableName).get(rowName).get("pos");

				if (seekPointerStr == null) {
					res.status(404, "Error - Requested row not found");
					return "404 Error - Requested row not found";
				}

				long seekPointer = Long.parseLong(seekPointerStr);

				try {
					tableFile.seek(seekPointer);
					rowObj = Row.readFrom(tableFile);
				} catch (IOException e1) {
					System.out.println("IOException while seeking table file to a pointer position");
					e1.printStackTrace();
				} catch (Exception e) {
					System.out.println("Exception while reading row from table file in handling put new data route");
					e.printStackTrace();
				}
			}
			res.bodyAsBytes(rowObj.toByteArray());

		} else {
			res.bodyAsBytes(KVStore.get(tableName).get(rowName).toByteArray());
		}

		return null;
	}

	private static String streamingRead(Request req, Response res) throws Exception {

		String tableName = req.params("T");
		String startRow = req.queryParams("startRow");
		String endRowExclusive = req.queryParams("endRowExclusive");

		res.type("text/plain");

		if (!KVStore.containsKey(tableName)) {
			res.status(404, "Error - Requested table not found");
			return "404 Error - Requested table not found";
		}

		ConcurrentHashMap<String, Row> table = KVStore.get(tableName);

		for (ConcurrentHashMap.Entry<String, Row> entry : table.entrySet()) {

			String rowName = entry.getKey();

			if (startRow != null && rowName.compareTo(startRow) < 0)
				continue;
			if (endRowExclusive != null && endRowExclusive.compareTo(rowName) <= 0)
				continue;

			if (persistentTableFile.containsKey(tableName)) {

				RandomAccessFile tableFile = persistentTableFile.get(tableName);
				Row rowObj = null;

				synchronized (tableFile) {
					String seekPointerStr = KVStore.get(tableName).get(rowName).get("pos");

					if (seekPointerStr == null) {
						continue;
					}

					long seekPointer = Long.parseLong(seekPointerStr);

					try {
						tableFile.seek(seekPointer);
						rowObj = Row.readFrom(tableFile);
					} catch (IOException e1) {
						System.out.println("IOException while seeking table file to a pointer position");
						e1.printStackTrace();
					} catch (Exception e) {
						System.out
								.println("Exception while reading row from table file in handling put new data route");
						e.printStackTrace();
					}
				}
				res.write(rowObj.toByteArray());
				res.write(new byte[] { LINE_FEED });

			} else {
				res.write(KVStore.get(tableName).get(rowName).toByteArray());
				res.write(new byte[] { LINE_FEED });
			}
		}

		res.write(new byte[] { LINE_FEED });

		return null;
	}

	private static String streamingWrite(Request req, Response res) {

		String tableName = req.params("T");
		byte[] writeRows = req.bodyAsBytes();

		if (!KVStore.containsKey(tableName)) {
			ConcurrentHashMap<String, Row> table = new ConcurrentHashMap<String, Row>();
			KVStore.put(tableName, table);

			Path path = Paths.get(STORAGE_DIRECTORY, tableName + ".table");
			RandomAccessFile tableFile;

			try {
				Files.createFile(path);
				tableFile = new RandomAccessFile(path.toString(), "rw");
				nonPersistentTableFile.put(tableName, tableFile);
			} catch (IOException e) {
				System.out.println("Error while creating a new file for non-persistent table.");
				e.printStackTrace();
			}
		}

		InputStream writeRowsStream = new ByteArrayInputStream(writeRows);

		Row rowObj;
		try {
			while ((rowObj = Row.readFrom(writeRowsStream)) != null) {

				String row = rowObj.key;

				Row rowObjInMem = null;
				if (!KVStore.get(tableName).containsKey(row)) {
					rowObjInMem = new Row(row);
					KVStore.get(tableName).put(row, rowObjInMem);
				} else {
					rowObjInMem = KVStore.get(tableName).get(row);
				}

				if (persistentTableFile.containsKey(tableName)) {

					RandomAccessFile tableFile = persistentTableFile.get(tableName);
					long fileLen = tableFile.length();
					Row prevRowObj = null;

					synchronized (tableFile) {
						String seekPointerStr = rowObjInMem.get("pos");

						// Get previous Row state

						if (seekPointerStr != null) {
							try {
								long seekPointer = Long.parseLong(seekPointerStr);
								tableFile.seek(seekPointer);
								prevRowObj = Row.readFrom(tableFile);
							} catch (IOException e1) {
								System.out.println("IOException while seeking table file to a pointer position");
								e1.printStackTrace();
							} catch (Exception e) {
								System.out.println(
										"Exception while reading row from table file in handling put new data route");
								e.printStackTrace();
							}
						} else {
							prevRowObj = new Row(row);
						}

					}

					// Add new columns in rowObj to this row
					for (String columnName : rowObj.columns()) {
						prevRowObj.put(columnName, rowObj.get(columnName));
					}

					// Put it at the end of .table file
					synchronized (tableFile) {
						tableFile.seek(fileLen);
						tableFile.write(prevRowObj.toByteArray());
						tableFile.writeByte(LINE_FEED);
					}

					// Update the seek pointer in KVStore
					rowObjInMem.put("pos", Long.toString(fileLen));

				} else {
					// Add new columns in rowObj to rowObjInMem
					for (String columnName : rowObj.columns()) {
						rowObjInMem.put(columnName, rowObj.get(columnName));
					}
				}
			}
		} catch (Exception e) {
			System.out.println("Exception occured while reading row from table file");
			e.printStackTrace();
		}

		return "OK";
	}

	private static String countRows(Request req, Response res) {

		String tableName = req.params("T");
		if (!KVStore.containsKey(tableName)) {
			res.status(404, "Error - Requested table not found");
			return "404 Error - Requested table not found";
		}

		int numRows = KVStore.get(tableName).size();

		res.body(String.valueOf(numRows));

		return null;
	}

	private static String getTablesList(Request req, Response res) {

		res.type("text/plain");
		String tableList = "";

		for (Map.Entry<String, ConcurrentHashMap<String, Row>> entry : KVStore.entrySet()) {
			tableList += entry.getKey() + "\n";
		}
		tableList += "\n";

		res.body(tableList);

		return null;
	}

	private static String renameTable(Request req, Response res) {

		String oldTableName = req.params("T");
		String newTableName = req.body();

		if (!KVStore.containsKey(oldTableName)) {
			res.status(404, "Error - Requested table to rename doesn't exist");
			return "Make sure the table to rename doesn't have any typos. Table names are case-sensitive";
		}

		if (KVStore.containsKey(newTableName)) {
			res.status(409, "Error - Table with requested new table name already exists");
			return "Choose an unused table name to rename";
		}

		synchronized (KVStore) {

			// Update KVStore
			KVStore.put(newTableName, KVStore.get(oldTableName));
			KVStore.remove(oldTableName);

			// Update file name and randomAccessFile pointer
			Path oldPath = Paths.get(STORAGE_DIRECTORY, oldTableName + ".table");
			Path newPath = Paths.get(STORAGE_DIRECTORY, newTableName + ".table");
			RandomAccessFile tableFile;
			try {
				Files.move(oldPath, newPath);
				tableFile = new RandomAccessFile(newPath.toString(), "rw");

				if (persistentTableFile.containsKey(oldTableName)) {
					persistentTableFile.put(newTableName, tableFile);
					persistentTableFile.remove(oldTableName);
				} else {
					nonPersistentTableFile.put(newTableName, tableFile);
					nonPersistentTableFile.remove(oldTableName);
				}
			} catch (IOException e) {
				System.out.println("Error while creating a new file for persistent table.");
				e.printStackTrace();
			}
		}

		res.status(200, "OK");
		return "OK";
	}

	private static String deleteTable(Request req, Response res) {

		String tableName = req.params("T");

		if (!KVStore.containsKey(tableName)) {
			res.status(404, "Error - Requested table to delete not found");
			return "404 Error - Requested table to delete not found";
		}

		synchronized (KVStore) {
			KVStore.remove(tableName);
			if (persistentTableFile.containsKey(tableName)) {
				persistentTableFile.remove(tableName);
			} else {
				nonPersistentTableFile.remove(tableName);
			}
		}

		try {
			Files.deleteIfExists(Paths.get(STORAGE_DIRECTORY, tableName + ".table"));
		} catch (IOException e) {
			System.out.println("IOException while deleting table file");
			e.printStackTrace();
		}

		res.status(200, "OK");
		return "OK";
	}

	public static void main(String args[]) throws Exception {

		if (args.length != 3) {
			System.out.println("Written by Shivani Reddy Rapole.");
			return;
		}

		SERVER_PORT = Integer.parseInt(args[0]);

		STORAGE_DIRECTORY = args[1];

		File storageDirectory = new File(STORAGE_DIRECTORY);

		if (!storageDirectory.exists()) {
			boolean folderCreated = storageDirectory.mkdirs();
			if (!folderCreated)
				System.out.println("Failed to create storage directory");
		}

		MASTER_ADDRESS = args[2];

		port(SERVER_PORT);

		startPingThread(SERVER_PORT, STORAGE_DIRECTORY, MASTER_ADDRESS);

		recoverTables();

		get("/", (req, res) -> {
			return getBasicTableInfo(req, res);
		});

		get("/view/:T", (req, res) -> {
			return getTableContent(req, res);
		});

		get("/data/:T/:R/:C", (req, res) -> {
			return handleGET(req.params("T"), req.params("R"), req.params("C"), req, res);
		});

		get("/data/:T/:R", (req, res) -> {
			return wholeRowRead(req, res);
		});

		get("/data/:T", (req, res) -> {
			return streamingRead(req, res);
		});

		get("/count/:T", (req, res) -> {
			return countRows(req, res);
		});

		get("/tables", (req, res) -> {
			return getTablesList(req, res);
		});

		put("/persist/:T", (req, res) -> {
			LAST_ACTIVE = System.currentTimeMillis();
			return addPersistentTable(req, res);
		});

		put("/data/:T/:R/:C", (req, res) -> {
			LAST_ACTIVE = System.currentTimeMillis();
			return handlePUT(req.params("T"), req.params("R"), req.params("C"), req, res);
		});

		put("/batchdata/:T/:R/:C", (req, res) -> {
			LAST_ACTIVE = System.currentTimeMillis();
			return handleBatchPUT(req.params("T"), req.params("R"), req.params("C"), req, res);
		});

		put("/data/:T", (req, res) -> {
			LAST_ACTIVE = System.currentTimeMillis();
			return streamingWrite(req, res);
		});

		put("/rename/:T", (req, res) -> {
			LAST_ACTIVE = System.currentTimeMillis();
			return renameTable(req, res);
		});

		put("/delete/:T", (req, res) -> {
			LAST_ACTIVE = System.currentTimeMillis();
			return deleteTable(req, res);
		});

		// GarbageCollector gc = new GarbageCollector();
		// gc.start();

		updateWorkerList uwl = new updateWorkerList();
		uwl.start();

		System.out.println("Worker listening on port " + SERVER_PORT);

	}

}

// private static String hashedStreamingRead(Request req, Response res) throws
// Exception {
//
// String tableName = req.params("T");
// String startRow = req.queryParams("startRow");
// String endRowExclusive = req.queryParams("endRowExclusive");
//
// if (!KVStore.containsKey(tableName)) {
// res.status(404, "Error - Requested table not found");
// return "404 Error - Requested table not found";
// }
//
// ConcurrentHashMap<String, Row> table = KVStore.get(tableName);
//
// for (ConcurrentHashMap.Entry<String, Row> entry : table.entrySet()) {
//
// String rowName = entry.getKey();
//
// if (startRow != null && rowName.compareTo(startRow) < 0)
// continue;
// if (endRowExclusive != null && endRowExclusive.compareTo(rowName) <= 0)
// continue;
//
// if (persistentTableFile.containsKey(tableName)) {
// String seekPointerStr = KVStore.get(tableName).get(rowName).get("pos");
//
// if (seekPointerStr == null) {
// continue;
// }
//
// long seekPointer = Long.parseLong(seekPointerStr);
// RandomAccessFile tableFile = persistentTableFile.get(tableName);
// Row rowObj = null;
//
// try {
// tableFile.seek(seekPointer);
// rowObj = Row.readFrom(tableFile);
// } catch (IOException e1) {
// System.out.println("IOException while seeking table file to a pointer
// position");
// e1.printStackTrace();
// } catch (Exception e) {
// System.out.println("Exception while reading row from table file in handling
// put new data route");
// e.printStackTrace();
// }
//
// String temp = rowName + " ";
// res.write(temp.getBytes());
// res.write(Integer.toString(rowObj.hashCode()).getBytes());
// res.write(new byte[] {LINE_FEED});
//
// } else {
// String temp = rowName + " ";
// res.write(temp.getBytes());
// res.write(Integer.toString(KVStore.get(tableName).get(rowName).hashCode()).getBytes());
// res.write(new byte[] {LINE_FEED});
// }
// }
//
// res.write(new byte[] {LINE_FEED});
//
// return null;
//
// }
//
//
