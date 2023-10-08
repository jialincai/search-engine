package cis5550.kvs;

import static cis5550.webserver.Server.get;

import static cis5550.webserver.Server.*;



class Master extends cis5550.generic.Master{
	
	public static String displayWorkerTable() {
		return "<!DOCTYPE html><html>"
				+ "<head> KVS Master Table "
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
				+ "	</style></head><body>" + workerTable() + "</body></html>";
	}
	
	public static void main(String args[]) throws Exception {
		if (args.length != 1) {
			System.out.println("Written by Shivani Reddy Rapole.");
			return;
		}
		
		int serverPort = Integer.parseInt(args[0]);
		
		port(serverPort);
		
		System.out.println("Master listening on port "+serverPort);
		
		get("/", (req,res) -> { return displayWorkerTable(); });
		
		registerRoutes();
	}
}