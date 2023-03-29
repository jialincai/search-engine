package cis5550.generic;

import java.io.*;
import java.net.*;
import java.util.*;

import cis5550.tools.Logger;
import cis5550.webserver.Request;
import cis5550.webserver.Response;
import cis5550.webserver.Server;

import static cis5550.webserver.Server.*;

public class Master {
    protected static int portNumber_;
    protected static List<WorkerInfo> activeWorkers_ = Collections.synchronizedList(new LinkedList<>());

    public static void setPort(int portNumber) {
        portNumber_ = portNumber;
        Server.port(portNumber);
    }

    /**
     * The HTML table with the list of workers.
     */
    public static String workerTable() throws IOException {
        // Read adminPage.html into a String.
        StringBuilder contentBuilder = new StringBuilder();
        BufferedReader in = new BufferedReader(new FileReader("src/cis5550/html/adminPage.html"));
        String str;
        while ((str = in.readLine()) != null) {
            contentBuilder.append(str);
        }
        in.close();
        String toReturn = contentBuilder.toString();

        // Html for worker attributes.
        contentBuilder.setLength(0);
        synchronized (activeWorkers_) {
            for (WorkerInfo w : activeWorkers_) {
                String link = String.format("http://%s:%d/", w.ip_, w.portNumber_);
                contentBuilder.append("<tr>")
                        .append(String.format("<td>%s</td>", w.id_))
                        .append(String.format("<td>%s</td>", w.ip_))
                        .append(String.format("<td>%d</td>", w.portNumber_))
                        .append(String.format("<td><a href=\"%s\">%s</td>", link, link))
                        .append("</tr>");
            }
        }
        toReturn = toReturn.replace("<!-- INSERT_TABLE_HERE -->", contentBuilder.toString());
        return toReturn;
    }

    /**
     * Creates routes for the /ping and /workers routes
     */
    public static void registerRoutes() {
        get("/ping", (req, res) -> {
            addWorker(req, res);
            return null;
        });

        get("/workers", (req, res) -> {
            return getWorkers();
        });
    }

    /**
     * Creates a thread that expires threads without pings in past 15 seconds.
     */
    public static void startExpireThread() {
        new Thread(() -> {
            while (true) {
                removeExpiredWorkers();
                try {
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e) {
                    ;
                }
            }
        }).start();
    }

    private static void removeExpiredWorkers() {
        activeWorkers_.removeIf(w -> System.currentTimeMillis() - w.lastPing_ > 15 * 1000);
    }

    private static void addWorker(Request request, Response response) throws UnknownHostException {
        String id = request.queryParams("id");
        String ip = request.ip();
        Integer portNumber;
        try {
            portNumber = Integer.parseInt(request.queryParams("port"));
        } catch (NumberFormatException e) {
            Logger.getLogger(Master.class).error("Unable to parse port number.", e);
            portNumber = null;
        }

        if (id != null || ip != null || portNumber != null) {
            WorkerInfo w = getWorker(id);
            if (w != null) {
                w.ip_ = ip;
                w.portNumber_ = portNumber;
                w.lastPing_ = System.currentTimeMillis();
            } else {
                activeWorkers_.add(new WorkerInfo(id, ip, portNumber));
            }
            response.status(200, "OK");
            response.body("OK");
        } else {
            response.status(400, "Bad Request");
        }
    }

    private static WorkerInfo getWorker(String id) {
        synchronized (activeWorkers_) {
            for (WorkerInfo worker : activeWorkers_) {
                if (worker.id_.equals(id)) {
                    return worker;
                }
            }
        }
        return null;
    }

    private static String getWorkers() {
        if (activeWorkers_.size() == 0) {
            return "0\n";
        }

        StringBuilder toReturn = new StringBuilder();
        toReturn.append(activeWorkers_.size());
        synchronized (activeWorkers_) {
            for (WorkerInfo worker : activeWorkers_) {
                if (System.currentTimeMillis() - worker.lastPing_ <= 15 * 1000) {
                    toReturn.append("\n");
                    toReturn.append(String.format("%s,%s:%d", worker.id_, worker.ip_, worker.portNumber_));
                }
            }
        }
        return toReturn.toString();
    }
}
