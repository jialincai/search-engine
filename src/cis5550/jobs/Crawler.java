package cis5550.jobs;

import java.io.*;
import java.net.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.*;

public class Crawler {
    public static void run(FlameContext _ctx, String _args[]) throws Exception {
        // Check the command-line arguments
        if (_args.length < 1) {
            _ctx.output("Syntax: Crawler <seedURL> <blacklistFile>");
            return;
        }

        URL seedURL = new URL(normalizeURL(_args[0]));
        FlameRDD urlsToVisit = _ctx.parallelize(Arrays.asList(seedURL.toString()));
        
        while (urlsToVisit.count() != 0) {
            urlsToVisit = urlsToVisit.flatMap(urlString -> {
                int responseCode;
                Map<String, List<String>> responseHeaders = new HashMap<>();
                boolean getPageContent = false;

                urlString = normalizeURL(urlString);
                URL url = new URL(urlString);
                KVSClient kvs = _ctx.getKVS();
                List<String> urlsToReturn = new ArrayList<>();

                // Return empty set if URL already in crawl table.
                if (kvs.existsRow("crawl", Hasher.hash(url.toString()))) {
                    return urlsToReturn;
                }
                
                Row newRow = new Row(Hasher.hash(url.toString()));
                newRow.put("url", url.toString());

                // Adhere to crawl-delay and robots.txt
                String[] urlParts = URLParser.parseURL(url.toString());
                String host = urlParts[1];
                // First time visiting this host, retrieve robots.txt
                if (!kvs.existsRow("hosts", Hasher.hash(host))) {
                    URL robotURL = new URL(String.format("%s://%s:%s/robots.txt", urlParts[0], urlParts[1], urlParts[2]));
                    HttpURLConnection RobotConnection = (HttpURLConnection) robotURL.openConnection();
                    RobotConnection.setRequestMethod("GET");
                    RobotConnection.setRequestProperty("User-Agent", "cis5550-crawler");
                    responseCode = RobotConnection.getResponseCode();

                    if (responseCode == 200) {
                        byte[] content = getResponseContent(RobotConnection);
                        kvs.put("hosts", Hasher.hash(host), "robots.txt", content);
                    }
                    kvs.put("hosts", Hasher.hash(host), "last-access", Integer.toString(0));
                }

                byte[] roboText = kvs.get("hosts", Hasher.hash(host), "robots.txt");
                if (canCrawl(roboText, urlString, _ctx)) {
                    Long crawlDelay = Long.parseLong(new String(_ctx.getKVS().get("hosts", Hasher.hash(host), "crawl-delay"), StandardCharsets.UTF_8)) * 1000;
                    Long lastAccess = Long.parseLong(new String(_ctx.getKVS().get("hosts", Hasher.hash(host), "last-access"), StandardCharsets.UTF_8));
                    if (System.currentTimeMillis() - lastAccess < crawlDelay) {
                        urlsToReturn.add(urlString);
                        return urlsToReturn;
                    }
                } else {
                    return urlsToReturn;
                }

                kvs.put("hosts", Hasher.hash(host), "last-access", Long.toString(System.currentTimeMillis()));

                // Send HEAD request and update column values.
                HttpURLConnection HEADconnection = (HttpURLConnection) url.openConnection();
                HEADconnection.setRequestMethod("HEAD");
                HEADconnection.setRequestProperty("User-Agent", "cis5550-crawler");

                responseCode = HEADconnection.getResponseCode();
                // Make response headers case-insensitive.
                Map<String, List<String>> caseSensitiveResponseHeaders = HEADconnection.getHeaderFields();
                for (var entry : caseSensitiveResponseHeaders.entrySet()) {
                    if (entry.getKey() != null) {
                        List<String> lowercaseList = entry.getValue().stream()
                                                                     .map(String::toLowerCase)
                                                                     .collect(Collectors.toList());
                        responseHeaders.put(entry.getKey().toLowerCase(), lowercaseList);
                    }
                }

                if (_ctx.getKVS().get("crawl", Hasher.hash(url.toString()), "responseCode") != null) {
                    byte[] result = _ctx.getKVS().get("crawl", Hasher.hash(url.toString()), "responseCode");
                    Integer.parseInt(new String(result, StandardCharsets.UTF_8));
                    newRow.put("responseCode", Integer.toString(responseCode));
                } else {
                    newRow.put("responseCode", Integer.toString(responseCode));
                }
                
                if (responseHeaders.get("content-type") != null) {
                    String type = responseHeaders.get("content-type").get(0);
                    newRow.put("contentType", type);
                    if (type.equals("text/html")) {
                        getPageContent = true;
                    }
                }
                if (responseHeaders.get("content-length") != null) {
                    newRow.put("length", responseHeaders.get("content-length").get(0));
                }

                // Handle redirects.
                List<Integer> redirectCodes = new ArrayList<>(Arrays.asList(301, 302, 303, 307, 308));
                if (redirectCodes.contains(responseCode) && responseHeaders.get("location") != null) {
                    urlsToReturn.add(normalizeURL(url.toString(), responseHeaders.get("location").get(0)));
                }
                
                HEADconnection.disconnect();

                // Send GET request and update column values.
                if (responseCode == 200 && getPageContent) {
                    HttpURLConnection GETconnection = (HttpURLConnection) url.openConnection();
                    GETconnection.setRequestMethod("GET");
                    GETconnection.setRequestProperty("User-Agent", "cis5550-crawler");
                    
                    responseCode = GETconnection.getResponseCode();
                    
                    byte[] pageContent = getResponseContent(GETconnection);
                    newRow.put("page", pageContent);
                    GETconnection.disconnect();
                    
                    urlsToReturn = extractURLs(pageContent);
                    for (int i = 0; i < urlsToReturn.size(); i++) {
                        urlsToReturn.set(i, normalizeURL(url.toString(), urlsToReturn.get(i)));
                    }
                }

                kvs.putRow("crawl", newRow);
                return urlsToReturn; 
		    });
            Thread.sleep(1000);
        }
        
        _ctx.output("OK");
    }

    private static String readLine(BufferedReader reader) {
        try {
            return reader.readLine();
        } catch (IOException e) {
            return null;
        }
    }

    private static String stripComments(String line) {
        int index = line.indexOf("#");
        if (index != -1) {
            return line.substring(0, index).trim();
        }
        return line.trim();
    }

    static boolean canCrawl(byte[] robotsContent, String url, FlameContext ctx) throws IOException {
        String [] urlParts = URLParser.parseURL(url);
        String robotsString = new String(robotsContent, StandardCharsets.UTF_8).toLowerCase();
        int startIndex;
        boolean canCrawl = true;
        boolean useDefaultDelay = true;

        // Get lines of robots.txt relavant to this crawler.
        Pattern cis5550Agent = Pattern.compile("user-agent\\s*:\\s*cis5550-crawler\\s*");
        Pattern wildAgent = Pattern.compile("user-agent\\s*:\\s*\\*\\s*");
        Matcher cis5550Matcher = cis5550Agent.matcher(robotsString);
        Matcher wildMatcher = wildAgent.matcher(robotsString);

        if (cis5550Matcher.find()) {
            startIndex = cis5550Matcher.start();
        } else if (wildMatcher.find()) {
            startIndex = wildMatcher.start();
        } else {
            // NO RULES FOUND
            ctx.getKVS().put("hosts", Hasher.hash(urlParts[1]), "crawl-delay", Integer.toString(1));
            return canCrawl;
        }

        int endIndex = robotsString.indexOf("user-agent:", startIndex + 1);
        if (endIndex != -1) {
            robotsString = robotsString.substring(startIndex, endIndex + 1);
        } else {
            robotsString = robotsString.substring(startIndex);
        }

        // Parse lines
        BufferedReader reader = new BufferedReader(new StringReader(robotsString));
        String line;
        while ((line = readLine(reader)) != null) {
            line = stripComments(line);
            if (line.length() == 0) {
                continue;
            }

            String[] pair = line.split(":");
            for (int i = 0; i < pair.length; i++) {
                pair[i] = pair[i].trim();
            }

            // TODO handle *, $, ? signs
            if (pair[0].equals("crawl-delay")) {
                ctx.getKVS().put("hosts", Hasher.hash(urlParts[1]), "crawl-delay", pair[1]);
                useDefaultDelay = false;
            } else if (url.indexOf(pair[1]) != -1) {
                if (pair[0].equals("disallow")) {
                    canCrawl = false;
                }
                break;
            }
        }

        if (useDefaultDelay) {
            ctx.getKVS().put("hosts", Hasher.hash(urlParts[1]), "crawl-delay", Integer.toString(1));
        }

        return canCrawl;
    }

    static byte[] getResponseContent(HttpURLConnection connection) throws IOException {
        byte[] content;
        
        InputStream in = connection.getInputStream();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = in.read(buffer)) != -1) {
            out.write(buffer, 0, bytesRead);
        }
        content = out.toByteArray();
        in.close();
        out.close();

        return content;
    }

    static List<String> extractURLs(byte[] html) {
        List<String> urlList = new ArrayList<>();
        
        String htmlString = new String(html, StandardCharsets.UTF_8);
        Pattern pattern = Pattern.compile("<(?!/).*?>");
        Matcher matcher = pattern.matcher(htmlString);

        while (matcher.find()) {
            String tag = matcher.group();
            String[] tagComponents = tag.substring(1, tag.length() - 1).split(" ");

            for (String component : tagComponents) {
                component = component.trim().toLowerCase();
            }

            if (tagComponents[0].equals("a")) {
                for (String component : tagComponents) {
                    if (component.indexOf("href=") == 0) {
                        String url = component.substring(5).trim().replaceAll("\"$", "").replaceAll("^\"", "");
                        urlList.add(url);
                    }
                }
            }
        }

        return urlList;
    }

    static String normalizeURL(String seed) {
        String[] urlParts = URLParser.parseURL(seed);
        if (urlParts[2] == null) {
            int port;
            if (urlParts[0].equals("http")) {
               port = 80;
            } else if (urlParts[0].equals("https")) {
                port = 443;
            } else {
                return null;
            }
            return String.format("%s://%s:%d%s", urlParts[0], urlParts[1], port, urlParts[3]);
        } else {
            return seed;
        }
    }

    static String normalizeURL(String base, String anchorLink) {
        // Strip #[remainder]
        if (anchorLink.indexOf("#") >= 0) {
            anchorLink = anchorLink.substring(0, anchorLink.indexOf("#") + 1);
        }
        if (anchorLink.isEmpty()) {
            return null;
        }

        String[] baseParts = URLParser.parseURL(base);
        String[] anchorParts = URLParser.parseURL(anchorLink);
        if (anchorParts[1] != null) {
            // External link - different host
            return normalizeURL(anchorLink);
        } else if (anchorParts[3].charAt(0) == '/') {
            // Absolute Link
            return String.format("%s://%s:%s%s", baseParts[0], baseParts[1], baseParts[2], anchorParts[3]);
        } else {
            // Relative Link
            baseParts[3] = baseParts[3].substring(0, baseParts[3].lastIndexOf("/"));
            
            String[] anchorLinkPath = anchorParts[3].split("/");
            String toAppend = "";
            for (String elem : anchorLinkPath) {
                if (elem.equals("..")) {
                    baseParts[3] = baseParts[3].substring(0, baseParts[3].lastIndexOf("/"));
                } else {
                    toAppend += "/" + elem;
                }
            }
            
            return String.format("%s://%s:%s%s", baseParts[0], baseParts[1], baseParts[2], baseParts[3] + toAppend);
        }
    }
}
