package cis5550.webserver;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class RequestParser {
    private String string;
    private String method;
    private String url;
    private HashMap<String, String> queryMap;
    private String protocol;
    private HashMap<String, String> headers;
    byte[] body;
    private boolean isValid;

    public RequestParser(ByteArrayOutputStream rawReq) {
        BufferedReader reqReader = new BufferedReader(
                new InputStreamReader(new ByteArrayInputStream(rawReq.toByteArray())));
        String line;

        string = rawReq.toString();
        queryMap = new HashMap<>();
        headers = new HashMap<>();
        try {
            line = reqReader.readLine();
            if (!parseMethod(line)) {
                isValid = false; // Method malformed
                return;
            }

            line = reqReader.readLine();
            while (!line.isEmpty()) {
                if (!parseHeader(line)) {
                    isValid = false; // Header malfromed
                    return;
                }
                line = reqReader.readLine();
            }
            isValid = true;
        } catch (IOException e) {
            isValid = false;
        }
    }

    public String asString() {
        return string;
    }

    public String getMethod() {
        return method;
    }

    public String getUrl() {
        return url;
    }

    public HashMap<String, String> getQueryMap() {
        return new HashMap<String, String>(queryMap);
    }

    public String getProtocol() {
        return protocol;
    }

    public HashMap<String, String> getHeaders() {
        return new HashMap<>(headers);
    }

    public byte[] getBody() {
        return body.clone();
    }

    public boolean isValid() {
        return isValid;
    }

    public void setBody(byte[] bodyArg) throws UnsupportedEncodingException {
        body = bodyArg;
        if (headers.get("content-type") != null
                && headers.get("content-type").equals("application/x-www-form-urlencoded")) {
            String queryString = new String(bodyArg, StandardCharsets.UTF_8);
            parseQueryString(queryString);
        }
    }

    private boolean parseMethod(String s) throws UnsupportedEncodingException {
        String[] request = s.split(" ", 3);
        if (request.length == 3) {
            method = request[0].trim().toLowerCase();
            url = extractQueryParamsFromUrl(request[1].trim());
            protocol = request[2].trim().toLowerCase();
        } else {
            return false;
        }
        return true;
    }

    private String extractQueryParamsFromUrl(String url) throws UnsupportedEncodingException {
        int queryStartIdx = url.indexOf('?');
        if (queryStartIdx == -1 || queryStartIdx == url.length() - 1) {
            return url;
        }

        String queryString = url.substring(url.indexOf('?') + 1);
        parseQueryString(queryString);
        return url.substring(0, queryStartIdx);
    }

    private void parseQueryString(String queryString) throws UnsupportedEncodingException {
        String[] queryTokens = queryString.split("&");
        for (String token : queryTokens) {
            int idxPairSplit = token.indexOf('=');
            if (idxPairSplit == -1) {
                String decodedKey = java.net.URLDecoder.decode(token, "UTF-8");
                queryMap.put(decodedKey, null);
            } else {
                String decodedKey = java.net.URLDecoder.decode(token.substring(0, idxPairSplit), "UTF-8");
                String decodedVal = java.net.URLDecoder.decode(token.substring(idxPairSplit + 1), "UTF-8");
                queryMap.put(decodedKey, decodedVal);
            }
        }
    }

    private boolean parseHeader(String s) {
        String[] header = s.split(":", 2);
        if (header.length == 2) {
            headers.put(header[0].toLowerCase().trim(), header[1].trim());
        } else {
            return false;
        }
        return true;
    }
}
