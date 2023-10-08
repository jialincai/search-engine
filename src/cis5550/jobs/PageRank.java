package cis5550.jobs;

import java.io.*;
import java.math.MathContext;
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

public class PageRank {
	
	private static int countOccurences(String input, char targetChar) {
		
		int count = 0;
		for (int i = 0; i < input.length(); i++) {
		    if (input.charAt(i) == targetChar) {
		        count++;
		    }
		}
		return count; 
	}

	
	private static List<String> extractURLs(String baseURL, String page, FlameContext flameContext) throws IOException {
		
		String ANCHOR_TAG = "a";
//		KVSClient kvs = flameContext.getKVS();
		
		List<String> extractedURLs = new ArrayList<String> ();
//		String page = new String(pageBytes, StandardCharsets.UTF_8);
		
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
	            	
            	}
            	
            	
            }
        }
        
        return extractedURLs;
	}

	
	private static boolean isBlacklisted(String url, List<Pattern> BLACKLISTED_PATTERNS) {
		
        if (BLACKLISTED_PATTERNS == null || BLACKLISTED_PATTERNS.size() == 0) {
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
				normalizedURL = parsedBaseURL[0] + "://" + parsedBaseURL[1] + ":" + parsedBaseURL[2] + parsedBaseURL[3];
				normalizedURLs.add(normalizedURL);
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
			
			if (normalizedURL.toLowerCase().endsWith(".jpg") 
					   || normalizedURL.toLowerCase().endsWith(".jpeg")
					   || normalizedURL.toLowerCase().endsWith(".gif")
					   || normalizedURL.toLowerCase().endsWith(".png")
					   || normalizedURL.toLowerCase().endsWith(".txt")) {
				
				continue;
			} else {
				
				String relativeLink = parsedNormalizedURL[3];
				if (relativeLink.length() > 0 && relativeLink.charAt(0) == '/') { // Absolute paths lacking hostname
					

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
			if (robotRules != null) {
				for (int i = 1; i < robotRules.size(); i+=2) {
					
					if(relativeURL.startsWith(robotRules.get(i))) {
						if (robotRules.get(i+1).equals("disallow")) {
							disallow = true;
						}
						break;
					}
			}
			}
			
			if(disallow)
				continue;
				
			normalizedURLs.add(normalizedURL);	
			
		}
		
		return normalizedURLs;
	}
	
	
	public static void run(FlameContext flameContext, String[] args) throws Exception {	
		
		if (args.length < 1) {
			System.out.println("Provide a convergence threshold for pagerank");
			return;
		}
		
		
			
		KVSClient kvs = flameContext.getKVS();
		
		double convergenceThresh = Double.parseDouble(args[0]);
		double convergePercentThresh = 100;
		if (args.length > 1) {
			convergePercentThresh = Double.parseDouble(args[1]);
		}
		
		System.out.println("convergePercentThresh="+convergePercentThresh);
		
		FlamePairRDD urlPageMapping = flameContext.fromTable("crawl", r ->  {	String url = r.get("url");
																				String page = r.get("page");
																				List<String> extractedURLs;
																				List<String> normalizedURLs = null;
																				try {
																					extractedURLs = extractURLs(url, page, flameContext);
																					normalizedURLs = normalizeURLs(url, extractedURLs, null, null, flameContext);
																				} catch (IOException e) {
																					e.printStackTrace();
																				}
																				String res = url;
																				res += ",\"1.0,1.0";
																				
																				Set<String> normalizedSet = new HashSet<>(normalizedURLs);
																				for (String normalizedURL : normalizedSet) {
																					res += "," + normalizedURL; 
																				}
																				res += "\"";	
																				return res;
																			})
				 								  .mapToPair( s -> new FlamePair(s.substring(0, s.indexOf(",")), s.substring(s.indexOf(",")+1)));
		
//		urlPageMapping.saveAsTable("state");
		System.out.println("urlPageMapping done");
		
		
		
		
		int itr = 0;
		while(true) {
		
			itr +=1;
			System.out.println("itr="+itr);
			
			
		
			FlamePairRDD transferRDD = urlPageMapping.flatMapToPair(p -> { String baseUrl = p._1();
			  															   String URLs = p._2();
			  															   String[] URLsplit = URLs.substring(1, URLs.length()-1).split(",");
			  															   double numURLs = URLsplit.length - 2 ;
			  															   double rC = Double.parseDouble(URLsplit[0]);
			  															   double rP = Double.parseDouble(URLsplit[1]);
			  															   
			  															   Map<String, Double> resMap = new HashMap<String, Double>();
			  															   
			  															   resMap.put(baseUrl, 0.0);
			  															   List<FlamePair> resList = new ArrayList<FlamePair>();
//			  															   resList.add(new FlamePair(baseUrl, Double.toString(0.0)));
			  															   System.out.println("baseUrl="+baseUrl+"  rank=0.0");
			  															   
			  															   for (int i = 2; i<URLsplit.length; i++) {
			  																   double rank = 0.85 * (rC/numURLs);
			  																   String url = URLsplit[i];
			  																   if (resMap.containsKey(url)) {
			  																	 resMap.put(url, resMap.get(url) + rank) ;
			  																   } else {
			  																	 resMap.put(url, rank) ;
			  																   }
//			  																   resList.add(new FlamePair(URLsplit[i], Double.toString(rank)));
			  																   System.out.println("URLsplit[i]="+URLsplit[i]+"  rank="+resMap.get(url));
			  																   		  																   
			  															   }
			  															   
			  															 
			  															 
			  															 for (Map.Entry<String,Double> entry : resMap.entrySet()) {
			  																resList.add(new FlamePair(entry.getKey(), Double.toString(entry.getValue())));
			  															 }
			  															   
//			  															 System.out.println();
			  															 return resList;
			  															 
			  								  							 });
			
			
			
			
			
			FlamePairRDD aggregatedTransferRDD  = transferRDD.foldByKey("0", (a,b) -> Double.toString(Double.parseDouble(a) + Double.parseDouble(b)));
			
			
			
			FlamePairRDD joinedRDD = urlPageMapping.join(aggregatedTransferRDD);
			
			
			
			FlamePairRDD newUrlPageMapping = joinedRDD.flatMapToPair(p -> {	String url = p._1();
															String joinedState = p._2();
															System.out.println("joinedState="+joinedState+"\n");
															String prevState = joinedState.substring(1, joinedState.indexOf("\",")); // Eg: 0.1,0.1,L 
															
															String newState = joinedState.substring(joinedState.indexOf("\",")+2); // Eg: 0.02125
																			System.out.println("newState="+newState);
															String[] prevStateArr = prevState.split(",");
															
															String finalState = "";
															// Eg: 0.1,0.1,L = rc,rp,L
															
															finalState = finalState + Double.toString(0.15 + Double.parseDouble(newState)) + ",";
															finalState = finalState + prevStateArr[0];
															String ranks = prevStateArr[0] + "," + prevStateArr[1] + ",";
															if (prevState.indexOf(ranks) != -1)
																finalState = finalState + "," + prevState.substring(prevState.indexOf(ranks) + ranks.length());
															finalState = "\"" + finalState + "\"";
															
//															System.out.println("url="+url+"  finalState="+finalState+"\n");
//															System.out.println("------------\n");
															return Arrays.asList(new FlamePair(url, finalState));
															
														 });
			
			
//			newUrlPageMapping.saveAsTable("updatedTable"+itr);
			kvs.put("convergence", "totalUrls", "value", "0");
			kvs.put("convergence", "convergedUrls", "value", "0");
			
			FlameRDD rankDiff = newUrlPageMapping.flatMap(p -> { String state = p._2(); // Eg: "0.1,0.1,L"
										  state = state.substring(1, state.length()-1);
										  String[] stateArr = state.split(",");
										  double curRank = Double.parseDouble(stateArr[0]);
										  double prevRank = Double.parseDouble(stateArr[1]);
										  
										  System.out.println("url="+p._1()+ "  prevRank=" +prevRank+"  ");
										  double diff = Math.abs(curRank-prevRank);
										  return Arrays.asList(Double.toString(diff));
										});
			
			rankDiff.saveAsTable("rankDiff"+itr);
			Iterator <Row> itr_diff = kvs.scan("rankDiff"+itr, null, null);
			
			while(itr_diff.hasNext()) {
				Row row = itr_diff.next();
				for (String column: row.columns()) {
					double diff = Double.parseDouble(row.get(column));
					try {
	   					if (diff <= convergenceThresh) {

								int converged = Integer.parseInt(new String(kvs.get("convergence", "convergedUrls", "value")));
								kvs.put("convergence", "convergedUrls", "value", Integer.toString(converged+1));
	   					}
	   					int total = Integer.parseInt(new String(kvs.get("convergence", "totalUrls", "value")));
						kvs.put("convergence", "totalUrls", "value", Integer.toString(total+1));
					} catch (NumberFormatException | IOException e) {
						e.printStackTrace();
					}
				}
			}
			
			
			
			
			
//			String temp =	rankDiff.fold("0", (a,b) -> {
//												System.out.println("a="+a+"  b="+b);
//												try {
//								   					if (Double.parseDouble(b) <= convergenceThresh) {
//	
//															int converged = Integer.parseInt(new String(kvs.get("convergence", "convergedUrls", "value")));
//															kvs.put("convergence", "convergedUrls", "value", Integer.toString(converged+1));
//														
//	//							   						return Double.toString(Double.parseDouble(a) + 1);
//								   					}
//								   					int total = Integer.parseInt(new String(kvs.get("convergence", "totalUrls", "value")));
//													kvs.put("convergence", "totalUrls", "value", Integer.toString(total+1));
//												} catch (NumberFormatException | IOException e) {
//													e.printStackTrace();
//												}
//							   					return a+b;
////							   					return Double.toString(Math.max(Double.parseDouble(a), Double.parseDouble(b)));
//						   						});
			
			double numConvergedUrls = Double.parseDouble(new String(kvs.get("convergence", "convergedUrls", "value")));
			
			double totalURLs = Double.parseDouble(new String(kvs.get("convergence", "totalUrls", "value")));
//			Double totalURLs = Double.parseDouble(rankDiff.fold("0", (a,b) -> Double.toString(Double.parseDouble(a) + 1)));
//			
			System.out.println("totalURLs="+totalURLs);
			
			System.out.println("numConvergedUrls="+numConvergedUrls);
			
			double convergePercent = (numConvergedUrls/totalURLs) *100.0;
			
			System.out.println("convergePercent="+convergePercent);
			if (convergePercent >= convergePercentThresh) {
				newUrlPageMapping.saveAsTable("state");
				transferRDD.saveAsTable("transfer");
				Iterator <Row> itr_final = kvs.scan("state", null, null);
				
				while(itr_final.hasNext()) {
					Row row = itr_final.next();
					for (String column: row.columns()) {
						kvs.put("pageranks", row.key(), "rank", row.get(column).split(",")[0].substring(1));
					}
				}
//				FlamePairRDD temp1 = newUrlPageMapping.flatMapToPair(p -> { if (p!=null) { System.out.println("In final lambda");
//																		String url = p._1();
//																		String state = p._2();
//																		String[] stateStr = state.split(",");
//																		kvs.put("pageranks", url, "rank", stateStr[0]); }
//																		List <FlamePair> temp = new ArrayList<FlamePair>();
//																		return temp;
//																	  });
				break;
			}
			
			urlPageMapping = newUrlPageMapping;
			
		}
		
	
		
		
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}
}