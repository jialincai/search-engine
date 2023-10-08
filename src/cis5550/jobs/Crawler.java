package cis5550.jobs;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;
//import cis5550.kvs.*;
//import cis5550.webserver.*;

public class Crawler {
	
	private static int totalCount = 0;
	private static int pageCount = 0;
	
	private static int countOccurences(String input, char targetChar) {
		
		int count = 0;
		for (int i = 0; i < input.length(); i++) {
		    if (input.charAt(i) == targetChar) {
		        count++;
		    }
		}
		return count; 
	}

	
	private static List<String> extractURLs(String baseURL, byte[] pageBytes, FlameContext flameContext) throws IOException {
		
		FileWriter fw = null;
		try {
			fw = new FileWriter("extractedShivani.txt",true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// **********************************
		
        PrintWriter out = new PrintWriter(fw);
		String ANCHOR_TAG = "a";
//		KVSClient kvs = flameContext.getKVS();
		
		List<String> extractedURLs = new ArrayList<String> ();
		String page = new String(pageBytes, StandardCharsets.UTF_8);
		
		Pattern pattern = Pattern.compile("<(?!/)[^>]*>", Pattern.DOTALL); // get opening tag
//		Pattern pattern = Pattern.compile("\\<(.*?)\\>"); // get opening tag
        Matcher matcher = pattern.matcher(page);
        
        while (matcher.find()) {
        	
            String tag = matcher.group();
            String tagContent = tag.substring(1, tag.length()-1);
            String[] tagContentSplit = tagContent.split(" ");
            
            if (tagContentSplit.length>0 && tagContentSplit[0].equalsIgnoreCase(ANCHOR_TAG)) {
            	
            	int i = 1;
            	while (i < tagContentSplit.length && !tagContentSplit[i].toLowerCase().startsWith("href"))
            		i += 1;
            	
            	if (i < tagContentSplit.length && tagContentSplit[i].toLowerCase().startsWith("href")) {
	            	String extractedURL = tagContentSplit[i].substring(tagContentSplit[i].indexOf("=")+1);
	            	extractedURL = extractedURL.substring(1, extractedURL.length()-1);
	            	extractedURLs.add(extractedURL);
	            	out.println(extractedURL);
	            	
//	            	int anchorContentBegin = matcher.end();
	            	
//	            	if(page.charAt(anchorContentBegin) != '<') {
//	            		
//	            		int anchorContentEnd = page.indexOf("</a", anchorContentBegin);
//	            		String anchorContent = page.substring(anchorContentBegin, anchorContentEnd);
//	            		
//	            		String colKey = "anchor:"+baseURL;
//	            		if (kvs.get("anchorContent", extractedURL, colKey) == null) {
//	            			kvs.put("anchorContent", extractedURL, colKey, anchorContent.getBytes());
//	            		} else {
//	            			
//	            			String prevAnchorContent = new String(kvs.get("anchorContent", extractedURL, colKey));
//	            			kvs.put("anchorContent", extractedURL, colKey, (prevAnchorContent+anchorContent).getBytes());
//	            		}
//	            		
//	            	}
            	}
            	
            	
            }
        }
        out.close();
        
        return extractedURLs;
	}

	
	private static boolean isBlacklisted(String url, List<Pattern> BLACKLISTED_PATTERNS) {
		
        if (BLACKLISTED_PATTERNS.size() == 0) {
            return false;
        }
        
        for (Pattern pattern : BLACKLISTED_PATTERNS) {
        	
            Matcher matcher = pattern.matcher(url);
            if (matcher.matches()) {           	
                return true;
            }
        }
        return false;
    }
	
	
	private static List<String> normalizeURLs(String baseURL, List<String> URLs, List<String> robotRules, List<Pattern> BLACKLISTED_PATTERNS, FlameContext flameContext) throws IOException {
		
		
		FileWriter fw = null;
		try {
			fw = new FileWriter("normalizedShivani.txt",true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// **********************************
		
        PrintWriter out = new PrintWriter(fw);
        
        FileWriter fw2 = null;
		try {
			fw2 = new FileWriter("preNormalizedShivani.txt",true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// **********************************
		
        PrintWriter outpre = new PrintWriter(fw2);
        
		String[] parsedBaseURL = URLParser.parseURL(baseURL);
		
		if(parsedBaseURL[2] == null || parsedBaseURL[2].equals("")) {
			if(parsedBaseURL[0].toLowerCase().equals("http"))
				parsedBaseURL[2] = "80";
			else 
				parsedBaseURL[2] = "443";
		}
		
		if (parsedBaseURL[3].indexOf("#") != -1)
			parsedBaseURL[3] = parsedBaseURL[3].substring(0, parsedBaseURL[3].indexOf("#"));
		
		List<String> normalizedURLs = new ArrayList<String>();
		
		for (String url: URLs) {
			
			String normalizedURL = url;
			
			if (normalizedURL!=null && normalizedURL.indexOf("#") != -1)
				normalizedURL = normalizedURL.substring(0, normalizedURL.indexOf("#"));
			
			if (normalizedURL!=null && normalizedURL.equals("") || normalizedURL.equals("/")) {
				continue;
			}
			
			String[] parsedNormalizedURL = URLParser.parseURL(normalizedURL);
			
			String baseURLprefix = "";
			
			if (parsedBaseURL[3]!=null && parsedBaseURL[3].lastIndexOf('/') > 0) {
				baseURLprefix = parsedBaseURL[0] + "://" + parsedBaseURL[1] + ":" + parsedBaseURL[2] + parsedBaseURL[3].substring(0, parsedBaseURL[3].lastIndexOf('/'));
			} else {
				baseURLprefix = parsedBaseURL[0] + "://" + parsedBaseURL[1] + ":" + parsedBaseURL[2] + parsedBaseURL[3];
			}
			
			if (parsedNormalizedURL[0]!=null &&
					!parsedNormalizedURL[0].equalsIgnoreCase("http") &&
					!parsedNormalizedURL[0].equalsIgnoreCase("https")) {
				continue;
			}
				
			
			if (parsedNormalizedURL[0] == null) {
				parsedNormalizedURL[0] = parsedBaseURL[0];
			}
			
			if (parsedNormalizedURL[1] == null) {
				parsedNormalizedURL[1] = parsedBaseURL[1];
			}
			
			if (parsedNormalizedURL[2] == null || parsedNormalizedURL[2] == "") {
				if (parsedNormalizedURL[0].equalsIgnoreCase("http")) {
					parsedNormalizedURL[2] = "80";
				} else {
					parsedNormalizedURL[2] = "443";
				}
			} 
			
//			if (parsedNormalizedURL[0]!=null && parsedNormalizedURL[0].equalsIgnoreCase("https")) {
//				
//				if (parsedNormalizedURL[2] == null || parsedNormalizedURL[2] == "") {
//					parsedNormalizedURL[2] = "80";
//				}
//				normalizedURL = parsedNormalizedURL[0] + "://" + parsedNormalizedURL[1] + ":" + parsedNormalizedURL[2] + parsedNormalizedURL[3];
//				
//			} else if (parsedNormalizedURL[0]!=null && parsedNormalizedURL[0].equalsIgnoreCase("http")) {
//				
//				if (parsedNormalizedURL[2] == null || parsedNormalizedURL[2] == "") {
//					parsedNormalizedURL[2] = "443";
//				}
//				normalizedURL = parsedNormalizedURL[0] + "://" + parsedNormalizedURL[1] + ":" + parsedNormalizedURL[2] + parsedNormalizedURL[3];
//				
//			} else 
			if (normalizedURL.toLowerCase().endsWith(".jpg") 
					   || normalizedURL.toLowerCase().endsWith(".jpeg")
					   || normalizedURL.toLowerCase().endsWith(".gif")
					   || normalizedURL.toLowerCase().endsWith(".png")
					   || normalizedURL.toLowerCase().endsWith(".txt")) {
				
				continue;
			} else {
				
				String relativeLink = parsedNormalizedURL[3];
				if (relativeLink.length() > 0 && relativeLink.charAt(0) == '/') { // Absolute paths lacking hostname
					
//					baseURLprefix = baseURLprefix.substring(0, baseURLprefix.indexOf('/', 8)); // third occurence of '/'
//					normalizedURL = baseURLprefix + normalizedURL;

				} else {
				
					while (relativeLink!=null && relativeLink.startsWith("..")) {
						
						if (countOccurences(baseURLprefix, '/') > 3)
							baseURLprefix = baseURLprefix.substring(0, baseURLprefix.lastIndexOf('/'));
						
						relativeLink = relativeLink.substring(relativeLink.indexOf('/'));
					}
					
					String baseURLRelative = URLParser.parseURL(baseURLprefix)[3];
					if (baseURLRelative!=null && baseURLRelative.endsWith("/")) {
						parsedNormalizedURL[3] = URLParser.parseURL(baseURLprefix)[3] + relativeLink;
						
//						relativeLink = baseURLprefix + relativeLink;
					}
					else
						parsedNormalizedURL[3] = baseURLRelative + "/" + relativeLink; // relative path	

				}
			}
			
			normalizedURL = parsedNormalizedURL[0] + "://" + parsedNormalizedURL[1] + ":" + parsedNormalizedURL[2] + parsedNormalizedURL[3];
			
			String[] parsedNURL = URLParser.parseURL(normalizedURL);
			
			String relativeURL = parsedNURL[3];
			
			if(isBlacklisted(normalizedURL, BLACKLISTED_PATTERNS)) {	
				System.out.println("Blacklisted URL : "+normalizedURL);
				continue;
			}
			
			boolean disallow = false;
			for (int i = 1; i < robotRules.size(); i+=2) {
				
				if(relativeURL.startsWith(robotRules.get(i))) {
					if (robotRules.get(i+1).equals("disallow")) {
						disallow = true;
					}
					break;
				}
			}
			
			if(disallow)
				continue;
			
//			KVSClient kvs = flameContext.getKVS();
			
//		/*
//		 	if (kvs.existsRow("crawl", Integer.toString(normalizedURL.hashCode()))) {
//		 		
//		 		Row row = kvs.getRow("crawl", Integer.toString(normalizedURL.hashCode()));
//		 		Row anchorRow = kvs.getRow("anchorContent", url);
//		 		
//		 		row.
//		 		kvs.putRow(baseURL, anchorRow)
//		 	}
//		 */
//			if(kvs.existsRow("anchorContent", url)) {
//				Row row = kvs.getRow("anchorContent", url);
//			}
				
			normalizedURLs.add(normalizedURL);	
			out.println(normalizedURL);
			outpre.println(baseURL);
			outpre.println(url);
			
		}
		
		out.close();
		outpre.close();
		return normalizedURLs;
	}

	
	private static List<String> getUserAgentRules(String robotBody, String userAgent) {
		      
		String crawlDelay = null;

		List<String> rules = new ArrayList<String>();
		rules.add(crawlDelay);
		
		if (robotBody.equals("#")) {		
			return rules;
		}
		
		int fromIndex = 0;
		String userAgentTag = "User-agent: ";
		String allowTag = "Allow: ";
		String disallowTag = "Disallow: ";
		String crawlDelayTag = "Crawl-delay: ";
		
		while (robotBody.indexOf(userAgentTag, fromIndex) != -1) {
			
			int index = robotBody.indexOf(userAgentTag, fromIndex);
			int beginAgent = index + userAgentTag.length();
			int endAgent = robotBody.indexOf("\n", index);
			String agent = robotBody.substring(beginAgent, endAgent);
			String agentInfo = "";
			
			
			if (robotBody.indexOf("\n\n", endAgent+1) == -1 && endAgent+1 < robotBody.length()) {
				agentInfo = robotBody.substring(endAgent+1);
			} else {
				agentInfo = robotBody.substring(endAgent+1, robotBody.indexOf("\n\n", endAgent+1));
			}
			
			agentInfo += "\n";
			
			if (agent.equalsIgnoreCase(userAgent) || agent.equals("*")) {
				
				while (agentInfo.length() > 0) {
					if (agentInfo.startsWith(allowTag) && !agentInfo.equals("\n")) {
						
						rules.add(agentInfo.substring(allowTag.length(), agentInfo.indexOf("\n")));
						rules.add("allow");
					} else if (agentInfo.startsWith(disallowTag)) {
	
						rules.add(agentInfo.substring(disallowTag.length(), agentInfo.indexOf("\n")));
						rules.add("disallow");
					} else if (agentInfo.startsWith(crawlDelayTag)) {
						rules.add(0, agentInfo.substring(crawlDelayTag.length(), agentInfo.indexOf("\n")));
					}
					
					if(agentInfo.indexOf("\n")!=-1 && agentInfo.indexOf("\n")+1 < agentInfo.length())
						agentInfo = agentInfo.substring(agentInfo.indexOf("\n")+1);
					else
						agentInfo = "";
				}
				
				if (agent.equalsIgnoreCase(userAgent)) {
					return rules;
				}
			} 
			
			fromIndex = endAgent;
			
		}
		
		return rules;
	}

		
	private static List<String> crawl(String URL, FlameContext flameContext, List<Pattern> BLACKLISTED_PATTERNS) throws IOException {
		
		FileWriter fw = null;
		try {
			fw = new FileWriter("finalShivaniURL.txt",true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// **********************************
		
        PrintWriter out = new PrintWriter(fw);
        
        FileWriter fw2 = null;
		try {
			fw2 = new FileWriter("finalShivaniResponse.txt",true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// **********************************
		
        PrintWriter outURL = new PrintWriter(fw);
        
		KVSClient kvs = flameContext.getKVS();
		List<String> normalizedURLs = new ArrayList<String>();
		
		
		String[] parsedURL = URLParser.parseURL(URL);
		
		if(parsedURL[2] == null || parsedURL[2].equals("")) {
			if(parsedURL[0].toLowerCase().equals("http"))
				parsedURL[2] = "80";
			else 
				parsedURL[2] = "443";
		}
		
		if (parsedURL[3].indexOf("#") != -1)
			parsedURL[3] = parsedURL[3].substring(0, parsedURL[3].indexOf("#"));
		
		
		String normalizedURL = parsedURL[0] + "://" + parsedURL[1] + ":" + parsedURL[2] + parsedURL[3];
		
		String rowKeyHash = Hasher.hash(normalizedURL);
		
		if (kvs.existsRow("crawl", rowKeyHash)) {
			return normalizedURLs;
		}
		
		if (parsedURL[1]!= null && !kvs.existsRow("hosts", parsedURL[1])) {
			
			String robotsURL = parsedURL[0] + "://" + parsedURL[1] + ":" + parsedURL[2] + "/robots.txt";
			System.out.println("URL: "+ normalizedURL+ "    robotsURL: " + robotsURL);
			URL robotsUrl = new URL(robotsURL);
			HttpURLConnection robotConn = (HttpURLConnection) robotsUrl.openConnection();
			robotConn.setRequestMethod("GET");
			robotConn.setRequestProperty("User-Agent", "cis5550-crawler");
			robotConn.connect();
			
			if (robotConn.getResponseCode() == HttpURLConnection.HTTP_OK) {
				
				InputStream inputStream = robotConn.getInputStream();	
				byte[] robotBody = inputStream.readAllBytes(); 	
				kvs.put("hosts", parsedURL[1], "robots", robotBody);
			} else {
				
				String dummyRobotBody = "#";
				kvs.put("hosts", parsedURL[1], "robots", dummyRobotBody.getBytes());
			}
			
		}
		
		byte[] hostRobotTextBytes = kvs.get("hosts", parsedURL[1], "robots");
		String hostRobotText = new String(hostRobotTextBytes);
		List<String> robotRules = getUserAgentRules(hostRobotText, "cis5550-crawler");
		
		
		long RATE_LIMIT_THRESH = 1000;
		if (robotRules.get(0) != null) {
			RATE_LIMIT_THRESH = Long.parseLong(robotRules.get(0))*1000;
		}
		
		if(parsedURL[1]!= null) {
			
			byte[] lastAccessedTimeBytes = kvs.get("hosts", parsedURL[1], "timeStamp");
			
			if (lastAccessedTimeBytes != null) {
				
				String lastAccessedTime = new String(lastAccessedTimeBytes);
				
				if (System.currentTimeMillis() - Long.parseLong(lastAccessedTime) < RATE_LIMIT_THRESH) {		
					normalizedURLs.add(normalizedURL);
					return normalizedURLs;
				}
			}
		}
		
		URL url = new URL(normalizedURL);
		Row row = new Row(rowKeyHash);
		row.put("url", normalizedURL);	
		out.println(normalizedURL);
		
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		
		conn.setRequestMethod("HEAD");
		conn.setRequestProperty("User-Agent", "cis5550-crawler");
		conn.setInstanceFollowRedirects(false);
		conn.connect();
		int responseCode = conn.getResponseCode();
		String contentType = conn.getHeaderField("Content-Type");
		String contentLength = conn.getHeaderField("Content-Length");
		
		
		if (contentType != null) 
			row.put("contentType", contentType);
		if (contentLength != null) 
			row.put("contentLength", contentLength);
		
		
		kvs.put("hosts", parsedURL[1], "timeStamp", String.valueOf(System.currentTimeMillis()));
			
		if (responseCode == HttpURLConnection.HTTP_OK && contentType.equals("text/html")) {
			
			conn.disconnect(); 
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("User-Agent", "cis5550-crawler");
			conn.setInstanceFollowRedirects(false);
			conn.connect();
			
			responseCode = conn.getResponseCode();
			row.put("responseCode", Integer.toString(responseCode));
			outURL.println(Integer.toString(responseCode));
			
			if (responseCode == HttpURLConnection.HTTP_OK) {
				
				InputStream inputStream = conn.getInputStream();

				byte[] pageBody = inputStream.readAllBytes(); 
				String pageBodyStr = new String(pageBody);
				
				if (kvs.existsRow("pagecontent", Hasher.hash(pageBodyStr))) {
					
					row.put("canonicalURL", kvs.get("pagecontent", Hasher.hash(pageBodyStr), "url"));
				} else {
					
					row.put("page", pageBody);		
					kvs.put("pagecontent", Hasher.hash(pageBodyStr), "url", normalizedURL.getBytes());
					pageCount += 1;
					System.out.println("pageCount="+pageCount);
					List<String> extractedURLs = extractURLs(normalizedURL, pageBody, flameContext);
					normalizedURLs = normalizeURLs(normalizedURL, extractedURLs, robotRules, BLACKLISTED_PATTERNS, flameContext);
				}

			}
			
		} else if (responseCode == 301 || responseCode == 302 || responseCode == 303 || responseCode == 307 || responseCode == 308) {
			
			row.put("responseCode", Integer.toString(responseCode));
			outURL.println(Integer.toString(responseCode));
			List<String> redirectedURL = new ArrayList<String>();
			redirectedURL.add(conn.getHeaderField("Location"));
			normalizedURLs = normalizeURLs(normalizedURL, redirectedURL, robotRules, BLACKLISTED_PATTERNS, flameContext);
		} else {
			row.put("responseCode", Integer.toString(responseCode));
			outURL.println(Integer.toString(responseCode));
		}
		
		totalCount += 1;
		System.out.println("totalCount="+totalCount);
		kvs.putRow("crawl", row);

		out.close();
		outURL.close();
		conn.disconnect();
		
		return normalizedURLs;
	}
	
	public static void run(FlameContext flameContext, String[] args) throws Exception {
		
		if (args.length <= 0) {
			flameContext.output("Provide atleast one URL (seed URL) to start crawling");
		} else {
			flameContext.output("OK");
		
		KVSClient kvs1 = flameContext.getKVS();
		List<Pattern> BLACKLISTED_PATTERNS = new ArrayList<Pattern>();
		
//		String pattern1 = "http*://advanced.*.cis5550.net:*/cZl/*.html";
//		pattern1 = pattern1.replace("*", ".*");
//		BLACKLISTED_PATTERNS.add(Pattern.compile(pattern1));
			
		if (args.length > 1) { // Blacklisted table exists
			
			String blackListTable = args[1];
			
			Iterator<Row> blackListRows = kvs1.scan(blackListTable);
			
			while(blackListRows.hasNext()) {
				
				Row row = blackListRows.next();
				if (row.get("pattern") != null) {
					String pattern = row.get("pattern");
					pattern = pattern.replace("*", ".*");
					BLACKLISTED_PATTERNS.add(Pattern.compile(pattern));
				}
			}
		}
		try {
		FlameRDD urlQueue = flameContext.parallelize(Arrays.asList(args[0]));
		
//		long duration = 11*60*1000; // duration in milliseconds (5 seconds)
//        Timer timer = new Timer();

//        // create a timer task to stop the program
//        TimerTask stopProgram = new TimerTask() {
//            public void run() {
//                System.out.println("Time's up! Stopping the program.");
//                System.exit(0); // abruptly stop the program
//            }
//        };
//
//        //start the timer task after the specified duration
//        timer.schedule(stopProgram, duration);
		
		while(urlQueue!= null && urlQueue.count() > 0) {
//				
//				urlQueue = urlQueue.flatMap(URL -> crawl(URL, flameContext, BLACKLISTED_PATTERNS));
//	//			Thread.sleep(100);
				urlQueue = urlQueue.flatMap(URL -> {
					FileWriter fw = null;
					try {
						fw = new FileWriter("finalShivaniURL.txt",true);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					// **********************************
					
			        PrintWriter out = new PrintWriter(fw);
			        
			        FileWriter fw2 = null;
					try {
						fw2 = new FileWriter("finalShivaniResponse.txt",true);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					// **********************************
					
			        PrintWriter outURL = new PrintWriter(fw);
//			        
					KVSClient kvs = flameContext.getKVS();
					List<String> normalizedURLs = new ArrayList<String>();
					
					
					String[] parsedURL = URLParser.parseURL(URL);
					
					if(parsedURL[2] == null || parsedURL[2].equals("")) {
						if(parsedURL[0].toLowerCase().equals("http"))
							parsedURL[2] = "80";
						else 
							parsedURL[2] = "443";
					}
					
					if (parsedURL[3].indexOf("#") != -1)
						parsedURL[3] = parsedURL[3].substring(0, parsedURL[3].indexOf("#"));
					
					
					String normalizedURL = parsedURL[0] + "://" + parsedURL[1] + ":" + parsedURL[2] + parsedURL[3];
					
					String rowKeyHash = Hasher.hash(normalizedURL);
					
					if (kvs.existsRow("crawl", rowKeyHash)) {
						return normalizedURLs;
					}
					
					if (parsedURL[1]!= null && !kvs.existsRow("hosts", parsedURL[1])) {
						
						String robotsURL = parsedURL[0] + "://" + parsedURL[1] + ":" + parsedURL[2] + "/robots.txt";
						System.out.println("URL: "+ normalizedURL+ "    robotsURL: " + robotsURL);
						URL robotsUrl = new URL(robotsURL);
						HttpURLConnection robotConn = (HttpURLConnection) robotsUrl.openConnection();
						robotConn.setRequestMethod("GET");
						robotConn.setRequestProperty("User-Agent", "cis5550-crawler");
						robotConn.connect();
						
						if (robotConn.getResponseCode() == HttpURLConnection.HTTP_OK) {
							
							InputStream inputStream = robotConn.getInputStream();	
							byte[] robotBody = inputStream.readAllBytes(); 	
							kvs.put("hosts", parsedURL[1], "robots", robotBody);
						} else {
							
							String dummyRobotBody = "#";
							kvs.put("hosts", parsedURL[1], "robots", dummyRobotBody.getBytes());
						}
						
					}
					
					byte[] hostRobotTextBytes = kvs.get("hosts", parsedURL[1], "robots");
					String hostRobotText = new String(hostRobotTextBytes);
					List<String> robotRules = getUserAgentRules(hostRobotText, "cis5550-crawler");
					
					
					long RATE_LIMIT_THRESH = 1000;
					if (robotRules.get(0) != null) {
						RATE_LIMIT_THRESH = Long.parseLong(robotRules.get(0))*1000;
					}
					
					if(parsedURL[1]!= null && parsedURL[1]!="" && kvs.existsRow("hosts", parsedURL[1])) {
						
						System.out.println("parsedURL[1]="+parsedURL[1]);
						byte[] lastAccessedTimeBytes = kvs.get("hosts", parsedURL[1], "timeStamp");
						
						if (lastAccessedTimeBytes != null) {
							
							String lastAccessedTime = new String(lastAccessedTimeBytes);
							
							if (System.currentTimeMillis() - Long.parseLong(lastAccessedTime) < RATE_LIMIT_THRESH) {		
								normalizedURLs.add(normalizedURL);
								return normalizedURLs;
							}
						}
					}
					
					URL url = new URL(normalizedURL);
					Row row = new Row(rowKeyHash);
					row.put("url", normalizedURL);	
					out.println(normalizedURL);
					
					HttpURLConnection conn = (HttpURLConnection) url.openConnection();
					
					conn.setRequestMethod("HEAD");
					conn.setRequestProperty("User-Agent", "cis5550-crawler");
					conn.setInstanceFollowRedirects(false);
					conn.connect();
					int responseCode = conn.getResponseCode();
					String contentType = conn.getHeaderField("Content-Type");
					String contentLength = conn.getHeaderField("Content-Length");
					
					
					if (contentType != null) 
						row.put("contentType", contentType);
					if (contentLength != null) 
						row.put("contentLength", contentLength);
					
					
					kvs.put("hosts", parsedURL[1], "timeStamp", String.valueOf(System.currentTimeMillis()));
						
					if (responseCode == HttpURLConnection.HTTP_OK && contentType.equals("text/html")) {
						
						conn.disconnect(); 
						conn = (HttpURLConnection) url.openConnection();
						conn.setRequestMethod("GET");
						conn.setRequestProperty("User-Agent", "cis5550-crawler");
						conn.setInstanceFollowRedirects(false);
						conn.connect();
						
						responseCode = conn.getResponseCode();
						row.put("responseCode", Integer.toString(responseCode));
						outURL.println(Integer.toString(responseCode));
						
						if (responseCode == HttpURLConnection.HTTP_OK) {
							
							InputStream inputStream = conn.getInputStream();
		
							byte[] pageBody = inputStream.readAllBytes(); 
							String pageBodyStr = new String(pageBody);
							
							if (kvs.existsRow("pagecontent", Hasher.hash(pageBodyStr))) {
								
								row.put("canonicalURL", kvs.get("pagecontent", Hasher.hash(pageBodyStr), "url"));
							} else {
								
								row.put("page", pageBody);		
								kvs.put("pagecontent", Hasher.hash(pageBodyStr), "url", normalizedURL.getBytes());
								pageCount += 1;
								System.out.println("pageCount="+pageCount);
								List<String> extractedURLs = extractURLs(normalizedURL, pageBody, flameContext);
								normalizedURLs = normalizeURLs(normalizedURL, extractedURLs, robotRules, BLACKLISTED_PATTERNS, flameContext);
							}
		
						}
						
					} else if (responseCode == 301 || responseCode == 302 || responseCode == 303 || responseCode == 307 || responseCode == 308) {
						
						row.put("responseCode", Integer.toString(responseCode));
						outURL.println(Integer.toString(responseCode));
						List<String> redirectedURL = new ArrayList<String>();
						redirectedURL.add(conn.getHeaderField("Location"));
						normalizedURLs = normalizeURLs(normalizedURL, redirectedURL, robotRules, BLACKLISTED_PATTERNS, flameContext);
					} else {
						row.put("responseCode", Integer.toString(responseCode));
						outURL.println(Integer.toString(responseCode));
					}
					
					totalCount += 1;
					System.out.println("totalCount="+totalCount);
					kvs.putRow("crawl", row);
		
					out.close();
					outURL.close();
					conn.disconnect();
					Thread.sleep(100);
					
					return (Iterable<String>)normalizedURLs;
				});
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		}
		
		return;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}
	
}
