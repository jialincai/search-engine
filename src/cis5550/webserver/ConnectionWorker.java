package cis5550.webserver;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;

import cis5550.tools.Logger;

import static cis5550.webserver.Constants.*;

public class ConnectionWorker extends Thread {
    private Socket sock;
    private SocketAddress sockAddr;
    private BufferedInputStream sockIn;
    private BufferedOutputStream sockOut;
    private boolean connOpen;

    private static Server serverInstance;
    private static List<Triplet<String, String, Route>> routingTable;
    private static BlockingQueue<Socket> queue;
    private static Logger logger;

    public ConnectionWorker(BlockingQueue<Socket> queue, Logger logger,
            List<Triplet<String, String, Route>> routingTable, Server serverInstance) {
        connOpen = false;
        if (ConnectionWorker.queue == null) {
            ConnectionWorker.queue = queue;
        }
        if (ConnectionWorker.logger == null) {
            ConnectionWorker.logger = logger;
        }
        if (ConnectionWorker.routingTable == null) {
            ConnectionWorker.routingTable = routingTable;
        }
        if (ConnectionWorker.serverInstance == null) {
            ConnectionWorker.serverInstance = serverInstance;
        }
    }

    public void run() {
        while (true) {
            resetSocketInfo();
            try {
                sock = queue.take();
                logger.info("Incoming connection from" + sock.getRemoteSocketAddress());
            } catch (InterruptedException e) {
                logger.warn("InterruptedException during queue.take()");
                continue;
            }

            try {
                sockAddr = sock.getRemoteSocketAddress();
                sockIn = new BufferedInputStream(sock.getInputStream());
                sockOut = new BufferedOutputStream(sock.getOutputStream());
                connOpen = true;
                serviceConnection();
            } catch (IOException e) {
                logger.warn("Failed to get Socket input/ouput stream.");
            }
        }
    }

    private void serviceConnection() {
        try {
            while (connOpen) {
                ByteArrayOutputStream rawReq = new ByteArrayOutputStream();
                ByteArrayOutputStream rawBody = new ByteArrayOutputStream();
                int[] prevFourBytes = new int[4];

                boolean reqRecv = false;
                boolean bodyUpdated = false;
                RequestParser reqParser = null;
                int bytesToRead = 0;
                while (!(reqRecv && bodyUpdated)) {
                    readByte(prevFourBytes);
                    if (prevFourBytes[3] != EOF) {
                        if (!reqRecv) { // read request
                            rawReq.write(prevFourBytes[3]);
                            if (isEndSequence(prevFourBytes)) {
                                reqRecv = true;
                                reqParser = new RequestParser(rawReq);
                                logger.info(sockAddr + " sent request\n\n" + reqParser.asString());
                                String contentLength = reqParser.getHeaders().get("content-length");
                                if (contentLength != null) {
                                    bytesToRead = Integer.parseInt(contentLength);
                                } else {
                                    bytesToRead = 0;
                                }
                                if (bytesToRead == 0) {
                                    reqParser.setBody(new byte[0]);
                                    bodyUpdated = true;
                                }
                            }
                        } else if (!bodyUpdated) { // read body
                            if (bytesToRead > 1) {
                                rawBody.write(prevFourBytes[3]);
                                bytesToRead--;
                            } else {
                                rawBody.write(prevFourBytes[3]);
                                reqParser.setBody(rawBody.toByteArray());
                                bodyUpdated = true;
                            }
                        }
                    } else {
                        connOpen = false;
                        break;
                    }
                }
                Request req = new RequestImpl(reqParser.getMethod(), reqParser.getUrl(),
                        reqParser.getProtocol(), reqParser.getHeaders(), reqParser.getQueryMap(), null,
                        (InetSocketAddress) sockAddr,
                        reqParser.body, serverInstance);
                Response res = new ResponseImpl(sockOut, connOpen);
                if (reqParser.isValid()) {
                    handleRequest(req, res);
                } else {
                    errorResponse(400, "Bad Request", res);
                }
                logger.info(sockAddr + " received response\n\n" + ((ResponseImpl) res).toString());
                sockOut.flush();
            }
            sock.close();
        } catch (IOException e) {
            logger.warn("serviceConnection() failed. Socket read/write error.");
        }
    }

    private void handleRequest(Request req, Response res) throws IOException {
        Triplet<String, String, Route> route = getRoute(req);
        if (route != null) {
            handleDynamicRequest(req, res, route);
        } else {
            handleStaticRequest(req, res);
        }
    }

    public void handleDynamicRequest(Request req, Response res, Triplet<String, String, Route> route)
            throws IOException {
        try {
            Object handle_return = route.getThird().handle(req, res);
            if (((ResponseImpl) res).isWritten()) {
                connOpen = false;
            } else {
                if (handle_return != null) {
                    String bodyString = handle_return.toString();
                    res.header("Server", "cai_server");
                    res.header("Content-Length", Integer.toString(bodyString.length()));
                    res.body(bodyString);
                }
                ((ResponseImpl) res).sendHeaders();
                ((ResponseImpl) res).sendBody();
            }
        } catch (Exception e) {
            if (((ResponseImpl) res).isWritten()) {
                connOpen = false;
            } else {
                errorResponse(500, "Internal Server Error", res);
            }
        }
        return;
    }

    public void handleStaticRequest(Request req, Response res) throws IOException {
        if (req.protocol().equals("http/1.1")) {
            if (req.requestMethod().equals("get")) {
                handleGetRequest(req, res, true);
            } else if (req.requestMethod().equals("head")) {
                handleGetRequest(req, res, false);
            } else if (req.requestMethod().equals("post")) {
                errorResponse(405, "Not Allowed", res);
            } else if (req.requestMethod().equals("put")) {
                errorResponse(405, "Not Allowed", res);
            } else {
                errorResponse(501, "Not Implemented", res);
            }
        } else {
            errorResponse(505, "HTTP Version Not Supported", res);
        }
    }

    private void handleGetRequest(Request req, Response res, boolean includeBody) throws IOException {
        String filePath = Server.staticFiles.getLocation() + req.url();
        File file = new File(filePath);
        try {
            if (!file.isFile()) {
                errorResponse(404, "Not Found", res);
            } else if (isValidModifedSince(req, file) == ModifiedState.ERR) {
                errorResponse(400, "Bad Request", res);
            } else if (!file.canRead() || filePath.contains("..")) {
                errorResponse(403, "Forbidden", res);
            } else {
                StringBuilder staticRes = new StringBuilder();
                staticRes.append("HTTP/1.1 200 OK" + CRLF);
                resAddContentType(req.url(), staticRes);
                staticRes.append("Server: cai_hw1_server" + CRLF);
                staticRes.append("Content-Length: " + file.length() + CRLF + CRLF);
                sockOut.write(staticRes.toString().getBytes(), 0, staticRes.length());
                if (includeBody) {
                    if (isValidModifedSince(req, file) == ModifiedState.TRUE) {
                        writeBody(file);
                    } else {
                        errorResponse(304, "Not Modified", res);
                    }
                }
                logger.info(sock.getRemoteSocketAddress() + " requested file " + req.url());
                logger.info(sock.getRemoteSocketAddress() + " recieved file " + filePath);
            }
        } catch (SecurityException e) {
            errorResponse(403, "Forbidden", res);
        }
    }

    private void writeBody(File file) throws IOException {
        BufferedInputStream fileIn = new BufferedInputStream(new FileInputStream(file));
        int b = fileIn.read();
        while (b != EOF) {
            sockOut.write(b);
            b = fileIn.read();
        }
        fileIn.close();
    }

    private ModifiedState isValidModifedSince(Request req, File file) {
        String timeString = req.headers("if-modified-since");
        if (timeString == null) {
            return ModifiedState.TRUE;
        }
        SimpleDateFormat timeFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
        timeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date localLastModified = new Date(file.lastModified());
        Date httpLastModified = null;
        try {
            httpLastModified = timeFormat.parse(timeString);
        } catch (ParseException e) {
            return ModifiedState.ERR;
        }
        if (httpLastModified.before(localLastModified)) {
            return ModifiedState.TRUE;
        } else if (httpLastModified.after(new Date())) {
            return ModifiedState.ERR;
        } else {
            return ModifiedState.FALSE;
        }
    }

    private void resAddContentType(String url, StringBuilder res) {
        if (url.endsWith(".jpg") || url.endsWith(".jpeg")) {
            res.append("Content-Type: image/jpeg" + CRLF);
        } else if (url.endsWith(".txt")) {
            res.append("Content-Type: text/plain" + CRLF);
        } else if (url.endsWith(".html")) {
            res.append("Content-Type: text/html" + CRLF);
        } else {
            res.append("Content-Type: application/octet-stream" + CRLF);
        }
    }

    private static Triplet<String, String, Route> getRoute(Request req) {
        for (Triplet<String, String, Route> route : routingTable) {
            if (route.getFirst().equals(req.requestMethod())
                    && pathPatternMatchesUrl(route.getSecond(), req.url(), req)) {
                return route;
            }
        }
        return null;
    }

    private void readByte(int[] prevFourBytes) throws IOException {
        for (int i = 0; i < 3; i++) {
            prevFourBytes[i] = prevFourBytes[i + 1];
        }
        prevFourBytes[3] = sockIn.read();
    }

    private void resetSocketInfo() {
        sock = null;
        sockAddr = null;
        sockIn = null;
        sockOut = null;
        connOpen = false;
    }

    private void errorResponse(int statusCode, String reasonPhrase, Response res) throws IOException {
        res.status(statusCode, reasonPhrase);
        String bodyString = String.format("%d %s", statusCode, reasonPhrase);
        res.body(bodyString);
        res.header("Server", "cai_server");
        res.type("text/plain");
        res.header("Content-Length", Integer.toString(bodyString.length()));
        ((ResponseImpl) res).sendHeaders();
        ((ResponseImpl) res).sendBody();
    }

    private static boolean pathPatternMatchesUrl(String pathPattern, String url, Request req) {
        String[] pathPatternTokens = pathPattern.split("/");
        String[] urlTokens = url.split("/");

        HashMap<String, String> paramMap = new HashMap<>();

        if (pathPatternTokens.length != urlTokens.length) {
            return false;
        } else {
            for (int i = 0; i < pathPatternTokens.length; i++) {
                if (pathPatternTokens[i].indexOf(':') == 0) {
                    paramMap.put(pathPatternTokens[i].substring(1), urlTokens[i]);
                } else if (pathPatternTokens[i].equals(urlTokens[i])) {
                    continue;
                } else {
                    return false;
                }
            }
            ((RequestImpl) req).setParams(paramMap);
            return true;
        }
    }

    private static boolean isEndSequence(int[] fourBytes) {
        return fourBytes[0] == 13 && fourBytes[1] == 10 && fourBytes[2] == 13 && fourBytes[3] == 10;
    }
}
