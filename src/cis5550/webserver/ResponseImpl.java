package cis5550.webserver;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

// We only need to set content-length.
// Content-type and other headers are set by the route handler.
// To string could just be a byteString -- when you call to string on it...
public class ResponseImpl implements Response {

    private int statusCode;
    private String reasonPhrase;
    private boolean hasBody;
    private byte[] body;
    private HashMap<String, List<String>> headers;
    private boolean written;
    BufferedOutputStream sockOut;
    boolean connOpen;

    public ResponseImpl(BufferedOutputStream sockOut, boolean connOpen) {
        statusCode = 200;
        reasonPhrase = "OK";
        hasBody = false;
        body = null;
        headers = new HashMap<>();
        written = false;
        this.sockOut = sockOut;
        this.connOpen = connOpen;
    }

    public boolean hasBody() {
        return hasBody;
    }

    public byte[] getBody() {
        return body.clone();
    }

    public boolean isWritten() {
        return written;
    }

    public String toString() {
        StringBuilder toSend = new StringBuilder();
        toSend.append(String.format("HTTP/1.1 %d %s\r\n", statusCode, reasonPhrase));
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            for (String arg : entry.getValue()) {
                toSend.append(String.format("%s: %s\r\n", entry.getKey(), arg));
            }
        }
        toSend.append("\r\n");
        return toSend.toString();
    }

    public void sendHeaders() throws IOException {
        StringBuilder toSend = new StringBuilder();
        toSend.append(String.format("HTTP/1.1 %d %s\r\n", statusCode, reasonPhrase));
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            for (String arg : entry.getValue()) {
                toSend.append(String.format("%s: %s\r\n", entry.getKey(), arg));
            }
        }
        toSend.append("\r\n");
        sockOut.write(toSend.toString().getBytes());
    }

    public void sendBody() throws IOException {
        if (hasBody) {
            sockOut.write(body);
        }
    }

    @Override
    public void body(String body) {
        if (!written) {
            if (body != null) {
                hasBody = true;
                this.body = body.getBytes();
            }
        }
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) { // You need to test this and handle this case.
        if (!written) {
            if (body != null) {
                hasBody = true;
                this.body = bodyArg;
            }
        }
    }

    @Override
    public void header(String name, String value) {
        if (!written) {
            List<String> valueList = headers.get(name);
            if (valueList != null) {
                valueList.add(value);
            } else {
                headers.put(name, new LinkedList<>(Arrays.asList(value)));
            }
        }
    }

    @Override
    public void type(String contentType) {
        if (!written) {
            headers.put("Content-Type", new LinkedList<>(Arrays.asList(contentType)));
        }
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        if (!written) {
            this.statusCode = statusCode;
            this.reasonPhrase = reasonPhrase;
        }
    }

    @Override
    public void write(byte[] b) throws Exception {
        if (!written) {
            header("Connection", "close");
            sendHeaders();
            sockOut.write(b);
            written = true;
        } else {
            sockOut.write(b);
        }
    }

    @Override
    public void redirect(String url, int responseCode) {
        // TODO Auto-generated method stub

    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {
        // TODO Auto-generated method stub

    }

}
