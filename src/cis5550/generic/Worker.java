package cis5550.generic;

import java.io.*;
import java.net.*;
import java.util.Random;

import cis5550.tools.Logger;
import cis5550.webserver.Server;

public class Worker {
    protected static String id_;
    protected static String ip_;
    protected static int portNumber_;

    protected static String storageDirectory_;
    protected static String masterIp_;
    protected static int masterPortNumber_;

    public static void port(int portNumber) {
        portNumber_ = portNumber;
        Server.port(portNumber);
    }

    /**
     * Creates a thread that makes the periodic /ping requests.
     */
    public static void startPingThread() {
        new Thread(() -> {
            try {
                Socket socket = new Socket(masterIp_, masterPortNumber_);
                while (true) {
                    pingMaster(socket);
                    Thread.sleep(5 * 1000);
                }
            } catch (IOException e) {
                Logger.getLogger(Worker.class).fatal("Failed to ping master server.", e);
            } catch (InterruptedException e) {
                ;
            }
        }).start();
    }

    private static void pingMaster(Socket socket) throws IOException, InterruptedException {
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream());
            out.print(String.format("GET /ping?id=%s&port=%d HTTP/1.1\r\nHost: localhost\r\n\r\n", id_, portNumber_));
            out.flush();
        } catch (UnknownHostException e) {
            Logger.getLogger(Worker.class).fatal("Invalid master IP address.", e);
            throw e;
        } catch (IOException e) {
            Logger.getLogger(Worker.class).fatal("Failed connection to master server.", e);
            throw e;
        }
    }

    protected static String randomWorkerId() {
        Random rand = new Random();
        String id = new String();
        for (int i = 0; i < 5; i++) {
            id += Character.toString((char) (97 + rand.nextInt(26)));
        }
        return id;
    }
}
