package cis5550.webserver;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import cis5550.tools.Logger;
import static cis5550.webserver.Constants.*;

public class Server extends Thread {
    private final int NUM_WORKERS = MAX_CONNECTION_WORKERS;
    private static int portNo = defaultServerPort;
    private static Server serverInstance = null;
    private static boolean serverRunning = false;
    private static List<Triplet<String, String, Route>> routingTable = new LinkedList<>();
    private static final Logger logger = Logger.getLogger(Server.class);

    public static class staticFiles {
        private static String location = null;

        public static void location(String path) {
            constructServerInstance();
            startServerThread();
            location = path;
        }

        public static String getLocation() {
            return location;
        }
    }

    public static void port(int n) {
        constructServerInstance();
        portNo = n;
    }

    public static void get(String url, Route function) {
        constructServerInstance();
        startServerThread();
        routingTable.add(new Triplet<String, String, Route>("get", url, function));
    }

    public static void post(String url, Route function) {
        constructServerInstance();
        startServerThread();
        routingTable.add(new Triplet<String, String, Route>("post", url, function));
    }

    public static void put(String url, Route function) {
        constructServerInstance();
        startServerThread();
        routingTable.add(new Triplet<String, String, Route>("put", url, function));
    }

    public void run() {
        try {
            BlockingQueue<Socket> queue = new LinkedBlockingQueue<>();
            ServerSocket listenSock = new ServerSocket(portNo); // Never closed because socket listens infinitely.
            List<ConnectionWorker> threadPool = new ArrayList<>();
            for (int i = 0; i < NUM_WORKERS; i++) {
                threadPool.add(i, new ConnectionWorker(queue, logger, routingTable, serverInstance));
                threadPool.get(i).start();
            }
            while (true) {
                try {
                    queue.put(listenSock.accept());
                } catch (InterruptedException e) {
                    logger.warn("InterruptedException during queue.put()");
                }
            }
            // listenSock.close();
        } catch (IOException e) {
            System.out.println("Unable to create listening socket");
            System.exit(1);
        }
    }

    private static void constructServerInstance() {
        if (serverInstance == null) {
            serverInstance = new Server();
        }
    }

    private static void startServerThread() {
        if (!serverRunning) {
            serverRunning = true;
            serverInstance.start();
        }
    }
}
