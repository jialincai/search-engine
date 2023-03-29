package cis5550.kvs;

import java.io.*;
import cis5550.tools.*;

import static cis5550.webserver.Server.*;

public class Master extends cis5550.generic.Master {

    public static void main(String[] args) throws Exception {
        try {
            port(Integer.parseInt(args[0]));
        } catch (NumberFormatException e) {
            Logger.getLogger(Master.class).fatal("Unable to parse port number.", e);
            throw e;
        } catch (ArrayIndexOutOfBoundsException e) {
            Logger.getLogger(Master.class).fatal("Invalid number of arguments.", e);
            throw e;
        }

        registerRoutes();

        get("/", (req, res) -> {
            return adminPage();
        });

        startExpireThread();
    }

    public static String adminPage() throws IOException {
        return workerTable();
    }
}
