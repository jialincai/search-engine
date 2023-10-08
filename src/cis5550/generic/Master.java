package cis5550.generic;

import static cis5550.webserver.Server.get;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.webserver.Response;
import cis5550.webserver.Request;

public class Master {

	static class workerNode {
		String id;
		String ip;
		int port;
		long lastActive;
	}

	static ConcurrentHashMap<String, workerNode> activeWorkerNodes = new ConcurrentHashMap<String, workerNode>();

	public static List<String> getWorkers() {
		List<String> curWorkersList = new ArrayList<String>();
		for (Map.Entry<String, workerNode> entry : activeWorkerNodes.entrySet()) {
			workerNode node = entry.getValue();
			if (System.currentTimeMillis() - node.lastActive <= 15000) {
				curWorkersList.add(node.ip + ":" + node.port);
			}
		}
		return curWorkersList;
	}

	public static String workerTable() {

		String htmlTable = "";
		int curNode = 0;
		for (Map.Entry<String, workerNode> entry : activeWorkerNodes.entrySet()) {
			workerNode node = entry.getValue();
			if (System.currentTimeMillis() - node.lastActive <= 15000) {
				curNode += 1;
				String nodeInfo = node.id + "," + node.ip + ":" + node.port;
				htmlTable += "<tr>"
						+ String.format("<td> %d </td>", curNode)
						+ String.format("<td><a href=\"http://%s:%s/\"> %s </a></td>", node.ip, node.port, node.id)
						+ String.format("<td> %s </td>", node.ip)
						+ String.format("<td> %s </td>", node.port)
						+ "</tr>";
			}
		}

		if (htmlTable == "") {
			return "No active worker nodes to display.";
		}

		htmlTable = "<table>"
				+ "<thead><tr>"
				+ "		<th>S.No</th>"
				+ "		<th>ID</th>"
				+ "		<th>IP</th>"
				+ "		<th>Port</th>"
				+ "	</tr></thead><tbody>"
				+ htmlTable + "</tbody></table>";

		return htmlTable;

	}

	private static String handlePingRoute(Request req, Response res) {
		if (req.queryParams("id") == null) {
			res.status(400, "Error: ID is missing");
			return "400 Error: ID is missing";
		}
		if (req.queryParams("port") == null) {
			res.status(400, "Error: ID is missing");
			return "400 Error: Port is missing";
		}

		String id = req.queryParams("id");
		int port = Integer.parseInt(req.queryParams("port"));
		if (activeWorkerNodes.containsKey(id)) {
			workerNode node = activeWorkerNodes.get(id);
			node.ip = req.ip();
			node.port = port;
			node.lastActive = System.currentTimeMillis();
			activeWorkerNodes.put(id, node);
		} else {
			workerNode node = new workerNode();
			node.id = id;
			node.ip = req.ip();
			node.port = port;
			node.lastActive = System.currentTimeMillis();
			activeWorkerNodes.put(id, node);
		}
		return "OK";
	}

	private static String getWorkersList(Request req, Response res) {
		String workersInfo = "";
		int activeNodes = 0;
		for (Map.Entry<String, workerNode> entry : activeWorkerNodes.entrySet()) {
			workerNode node = entry.getValue();
			if (System.currentTimeMillis() - node.lastActive <= 15000) {
				activeNodes += 1;
				String nodeInfo = node.id + "," + node.ip + ":" + node.port;
				workersInfo += nodeInfo;
				workersInfo += "\n";
			}
		}

		workersInfo = Integer.toString(activeNodes) + "\n" + workersInfo;
		return workersInfo;
	}

	public static void registerRoutes() {

		get("/ping", (req, res) -> {
			return handlePingRoute(req, res);
		});

		get("/workers", (req, res) -> {
			return getWorkersList(req, res);
		});

	}

	public static void main() {

		Runnable removeInactiveWorkers = () -> {

			for (Map.Entry<String, workerNode> entry : activeWorkerNodes.entrySet()) {
				workerNode node = entry.getValue();
				if (System.currentTimeMillis() - node.lastActive > 15000) {
					activeWorkerNodes.remove(node.id);
				}
			}

			try {
				Thread.sleep(1500);
			} catch (InterruptedException e) {
				System.out.println("Error at thread sleep in removeInactiveWorkers");
				e.printStackTrace();
			}
		};

		Thread thread = new Thread(removeInactiveWorkers);
		thread.start();
	}

}