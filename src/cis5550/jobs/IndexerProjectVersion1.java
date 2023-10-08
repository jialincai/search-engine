package cis5550.jobs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Stemmer;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

public class IndexerProjectVersion1 {

	public static String CRAWLTABLENAME = "visitphilly1502";
	public static String docIdTableName = "docIdMapping";
	public static String docCountSoFarTable = "docCount";
	public static String indexTable = "index";
	public static String indexCountSoFarTable = "indexCount";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public static class Result {
		String docId;
		Double value;
		Result(String id, Double d) {
			docId = id;
			value = d;
		}
	}
	
	public static void batchProcessing(String indexTable, Map<String, Integer> countMap, KVSClient kvs, String urlId) {
		try {
			
			String rowString = "";
			String colString = "";
			String valueString = "";
			
			String seperator =  "~`~";
			
			int ptr = 1;
			
			for (String word: countMap.keySet()) {
			
				Integer count = countMap.get(word);
			
				if (kvs.existsRow(indexTable, word)) {
					
					Row wordRow = kvs.getRow(indexTable, word);
					
					if (wordRow.get(urlId) == null) {
						
						if (rowString.equals("")) {
							rowString = rowString + word;
						} else {
							rowString = rowString + seperator + word ;
						}
						
						if (colString.equals("")) {
							colString = colString + urlId;
						} else {
							colString = colString + seperator + urlId;
						}
						
						if (valueString.equals("")) {
							valueString = valueString + String.valueOf(count);
						} else {
							valueString = valueString + seperator + String.valueOf(count);
						}
						
						// Add document in wordlist 
						
						String currentDocList = wordRow.get("doclist");
						String updatedDocList = currentDocList + "," + urlId;
						
						rowString = rowString + seperator + word;
						colString = colString + seperator + "doclist";
						valueString = valueString + seperator + updatedDocList;
						
					} else {
						
						String updatedCount = String.valueOf(Integer.valueOf(wordRow.get(urlId)) + count);
						if (rowString.equals("")) {
							rowString = rowString + word;
						} else {
							rowString = rowString + seperator + word;
						}
						
						if (colString.equals("")) {
							colString = colString + urlId;
						} else {
							colString = colString + seperator + urlId;
						}
						
						if (valueString.equals("")) {
							valueString = valueString + String.valueOf(updatedCount);
						} else {
							valueString = valueString + seperator + String.valueOf(updatedCount);
						}
						
					}
					
				} else {
					
					// Word does not exist in the database itself. 
					if (rowString.equals("")) {
						rowString = rowString + word;
					} else {
						rowString = rowString + seperator + word;
					}
					
					if (colString.equals("")) {
						colString = colString + urlId;
					} else {
						colString = colString + seperator + urlId;
					}
					
					if (valueString.equals("")) {
						valueString = valueString + String.valueOf(count) ;
					} else {
						valueString = valueString + seperator + String.valueOf(count);
					}
					
					rowString = rowString + seperator + word;
					colString = colString + seperator + "doclist";
					valueString = valueString + seperator + urlId;
				}
				
				if (ptr > 100) {
					kvs.batchPut(indexTable, rowString, colString, valueString);
					ptr = 1;
					rowString = "";
					colString = "";
					valueString = "";
				}
				
			}
			
			
			/*System.out.println("\n****************");
			
			String[] rowList = rowString.split(seperator);
			String[] colList = colString.split(seperator);
			String[] valList = valueString.split(seperator);
			
			System.out.println("\n-------------------------------------");
			
			System.out.println("rowList.length="+rowList.length);
			System.out.println("colList.length="+colList.length);
			System.out.println("valList.length="+valList.length);
			System.out.println();
			
			System.out.println("\n-------------------------------------");
			
			for (int i=0; i<rowList.length ; i++) {
				if (i >= valList.length) {
					
					System.out.println("DEBUGGING ---------------------");
					
					System.out.println("row="+rowList[i]+"  col="+colList[i]);
//					System.out.println("row="+rowList[i]+"  col="+colList[i]+"  val="+valList[i]);
				} else {
					System.out.println("Row: " + rowList[i] + " Col: " + colList[i] + " Value: " + valList[i]);
				}
			}
			
			System.out.println("******************\n");
			*/
			
			//kvs.batchPut(indexTable, rowString, colString, valueString);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	/*public static void addWordToIndex(String word, String urlId, String indexTable, KVSClient kvs, int wordPos) {

		try {
			
			Row wordRow = kvs.getRow(indexTable, word);

			// Case - This word has been seen before.
			if (wordRow != null) {

				if (wordRow.get(urlId) == null) {

					// This word has not been seen with this document before.
					kvs.put(indexTable, word, urlId, "1:" + wordPos);
					byte[] docListSoFar = kvs.get(indexTable, word, "doclist");
					String docList = new String(docListSoFar, StandardCharsets.UTF_8);
					String updatedDocList = docList + "," + urlId;
					kvs.put(indexTable, word, "doclist", updatedDocList);

				} else {

					// This word has been seen with this document before.
					String countString = wordRow.get(urlId);
					int colonIndex = countString.indexOf(":");
					
					String countOfWordInDocumentSoFar = countString.substring(0, colonIndex);
					String positionSoFar = countString.substring(colonIndex+1);
					
					Integer updatedCount = Integer.valueOf(countOfWordInDocumentSoFar) + 1;
					String updatedPositionString = positionSoFar + "," + wordPos;
					
					String valueString = String.valueOf(updatedCount) + ":" + updatedPositionString;
					
					kvs.put(indexTable, word, urlId, valueString);

				}

			}
			// Case - This word has never been seen before.
			else {
				
				kvs.put(indexTable, word, urlId, "1:" + wordPos);
				kvs.put(indexTable, word, "doclist", urlId);
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	*/

	public static Map<String, Double> ranker(KVSClient k, String indexTable, String queryString) {
		
		// Process query string the same way.
		queryString = queryString.replaceAll("\\<.*?\\>", " ");
		queryString = queryString.replaceAll("[.,:;!?'\"()-]", " ");
		queryString = queryString.replaceAll("[\r\n\t]", " ");
		queryString = queryString.toLowerCase();

		String[] queryStringSplit = queryString.split("\\s+");

		Map<String, Double> rankings = new HashMap<>();

		try {
			for (String word : queryStringSplit) {
				
				if (k.existsRow(indexTable, word)) {
					
					Row r = k.getRow(indexTable, word);
					String docList = r.get("doclist");
					String[] allDocuments = docList.split(",");

					for (String doc : allDocuments) {
						String docValueString = r.get(doc);
						String tf = docValueString.substring(0, docValueString.indexOf(":"));
						
						String idf = r.get("idf");
						Double docWeight = (1 + Math.log10(Double.parseDouble(tf))) * Double.parseDouble(idf);
						
						if (rankings.containsKey(doc)) {
							double currValueForDoc = rankings.get(doc);
							double updatedValueForDoc = currValueForDoc + docWeight;
							rankings.put(doc, updatedValueForDoc);
						} else {
							rankings.put(doc, docWeight);
						}
					}
					
				}
				
			}
			
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return rankings;

	}
    
	public static TreeMap<String, Double> mergeRanks(KVSClient k, Map<String, Double> tfIdfRankings) throws Exception {
		
		TreeMap<String, Double>  finalRanks = new TreeMap<String, Double> ();
		for (Map.Entry<String, Double> entry: tfIdfRankings.entrySet()) {
			
			System.out.println(entry.getKey());
			System.out.println(entry.getValue());
			String docID = entry.getKey();
			String docURL = new String(k.get("docIdMapping", docID, "mapping"), StandardCharsets.UTF_8);
			byte[] pageRankBytes = k.get("pageranks", docURL, "rank");
			if(pageRankBytes == null)
				continue;
			Double pageRank = Double.parseDouble(new String(pageRankBytes, StandardCharsets.UTF_8));
			
			System.out.println("docID="+docID+"   pgrnk="+pageRank);
			Double tfIdfRank = entry.getValue();
			Double finalRank = (2 * pageRank * tfIdfRank)/(pageRank + tfIdfRank);
			finalRanks.put(docURL, finalRank);

		}
		return finalRanks;
	}
	
	public static class phraseQueryObject {
		String term;
		String queryPos;
		String df;
		phraseQueryObject(String t, String pos, String d) {
			term = t;
			queryPos =  pos;
			df = d;
		}
		
	}
	
	/*static boolean checkExactMatch(KVSClient k, String indexTable, String docId, String currentWord, String posCurrent, String previousWord, String posPrevious) {
		
		boolean foundMatch = false;
		
		try {
			String[] word1List = new String(k.get(indexTable, currentWord, docId), StandardCharsets.UTF_8).split(":")[1].split(",");
			String[] word2List = new String(k.get(indexTable, previousWord, docId), StandardCharsets.UTF_8).split(":")[1].split(",");

			int i = 0;
			int j = 0;
			int m = word1List.length;
			int n = word2List.length;
			
			int expectedPosDiff = Integer.valueOf(posCurrent) - Integer.valueOf(posPrevious);
			
			while (i<m && j<n) {
				int pos1 = Integer.valueOf(word1List[i]);
				int pos2 = Integer.valueOf(word2List[j]);
				
				int posDiff = pos1 - pos2;
				if (posDiff == expectedPosDiff) {
					foundMatch = true;
					break;
				} else if (posDiff < expectedPosDiff) {
					i++;
				} else {
					j++;
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return foundMatch;
	}
	
	public static List<String> phraseSearch(KVSClient k, String indexTable, String queryString) {
		
		List<String> matches = new ArrayList<>();
		List<String> docIntersection = new ArrayList<>();
		
		List<phraseQueryObject> phraseWords = new ArrayList<>();
		
		int wordPos = 1;
		
		queryString = queryString.replaceAll("\\<.*?\\>", " ");
		queryString = queryString.replaceAll("[.,:;!?'\"()-]", " ");
		queryString = queryString.replaceAll("[\r\n\t]", " ");
		queryString = queryString.toLowerCase();

		String[] queryWords = queryString.split("\\s+");
		
		try {
			for (String word: queryWords) {
				Row r = k.getRow(indexTable, word);
				String df = r.get("df");
				phraseWords.add(new phraseQueryObject(word, String.valueOf(wordPos), df));
				wordPos = wordPos+1;
			} 
			
			// If there is only one word in the query string return the empty list back.
			if (wordPos == 2) {
				return matches;
			}
			
			// Sort phrase words based on increasing order of document frequency
			Collections.sort(phraseWords, new Comparator<phraseQueryObject>() {
	            @Override
	            public int compare(phraseQueryObject p1, phraseQueryObject p2) {
	                return Integer.valueOf(p1.df) - Integer.valueOf(p2.df);
	            }
	        });
			
			phraseQueryObject firstWordObject = phraseWords.get(0);
			String firstWord = firstWordObject.term;
			Row firstWordRowObject = k.getRow(indexTable, firstWord);
			String firstWordDocList = firstWordRowObject.get("doclist");
			
			docIntersection = Arrays.asList(firstWordDocList.split(","));
			
			for (int i=1; i<phraseWords.size(); i++) {
				
				List<String>tempIntersection = new ArrayList<>();
				
				phraseQueryObject currentObject = phraseWords.get(i);
				String currentWord = currentObject.term;
				Row currentWordRowObject = k.getRow(indexTable, currentWord);
				String currentDocList = currentWordRowObject.get("doclist");
				List<String> currentDocs = Arrays.asList(currentDocList.split(","));
				
				for (String doc: docIntersection) {
					if (currentDocs.contains(doc)) {
						tempIntersection.add(doc);
					}
				}
				
				docIntersection = tempIntersection;
				
			}
				
			for (int i=1; i<phraseWords.size(); i++) {
				phraseQueryObject currentObject = phraseWords.get(i);
				String currentWord = currentObject.term;
				String posCurrent = currentObject.queryPos;
				Row currentWordRowObject = k.getRow(indexTable, currentWord);
				
				phraseQueryObject previousObject = phraseWords.get(i-1);
				String previousWord = previousObject.term;
				String posPrevious = previousObject.queryPos;
				
				// For these 2 words check which documents have these words in the correct order. 
				// Update the docIntersection with those documents which have these as exact matches. 
				// In the correct order.
				
				List<String> matchedDocuments = new ArrayList<>();
				
				for (String docId: docIntersection) {
					boolean matchFound = checkExactMatch(k, indexTable, docId, currentWord, posCurrent, previousWord, posPrevious);
					if (matchFound == true) {
						matchedDocuments.add(docId);
					}
				}
				
				docIntersection = matchedDocuments;
				
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return docIntersection;

	}
	*/
	
	static boolean onlyDigits(String zipString) {
		String regex = "[0-9]+";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(zipString);
        return m.matches();
	}
	

	public static void run(FlameContext fc, String[] args) {

		try {

			KVSClient k = fc.getKVS();
			String kvsMasterPort = fc.getKVS().getMaster();

			
			k.put(docCountSoFarTable, "count", "count", String.valueOf(0));
			
			k.delete(indexTable);
			k.delete(docIdTableName);
			
			long time1 = System.currentTimeMillis();
			
			Iterator<Row> itr = k.scan(CRAWLTABLENAME, null, null);
			
			while (itr.hasNext()) {
				
				Row r = itr.next();
				String url = r.get("url");
				
//				if (!url.contains("..")) {
					String urlDocId = null;
					
					Row countRow = k.getRow(docCountSoFarTable, "count");
					urlDocId = countRow.get("count");
					Integer idSoFar = Integer.valueOf(urlDocId);
					idSoFar = idSoFar + 1;
					urlDocId = String.valueOf(idSoFar);
					
					String encodedUrl = java.net.URLEncoder.encode(url,"UTF-8");
					
					k.put(docCountSoFarTable, "count", "count", urlDocId);
					k.put(docIdTableName, urlDocId, "mapping", encodedUrl);
					k.put(docIdTableName, encodedUrl, "mapping", urlDocId);
					
//				} 	
			}
			
			long time2 = System.currentTimeMillis();
			
			System.out.println("Doc ID Mapping took: " + (time2-time1)/1000 + " seconds.");
			
			Thread.sleep(5000);
			time2 = System.currentTimeMillis();
			
			
//			Iterator<Row> rowItr = k.scan(CRAWLTABLENAME, null, null);
//			
//			while (rowItr.hasNext()) {
//				
//				Row r = rowItr.next();
//				
//				String url = r.get("url");
//				String encodedUrl = null;
//				try {
//					encodedUrl = java.net.URLEncoder.encode(url,"UTF-8");
//				} catch (UnsupportedEncodingException e1) {
//					e1.printStackTrace();
//				}
//				System.out.println("Got values for url: " + encodedUrl);
//				String page = r.get("page");
//				
//				System.out.println("Got page for url: " + url);
//
//				
//				KVSClient kvs = new KVSClient(kvsMasterPort);
//					String urlDocId = null;
//	
//					try {
//						if (kvs.existsRow(docIdTableName, encodedUrl)) {
//							System.out.println("Found doc here");
//							
//							Row row = kvs.getRow(docIdTableName, encodedUrl);
//							urlDocId = row.get("mapping");
//							
//							System.out.println("Found mapping for the doc here");
//							
//							kvs.put("stage1", urlDocId, "col", urlDocId + "," + page);
//						} 
//	
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
//					
//					System.out.println("Did not find");
//					
////					return "";
//				
//			}
			
			FlameRDD urlPageRDD = fc.fromTable(CRAWLTABLENAME, r -> {
				
//				System.out.println("Here safely");
				String url = r.get("url");
				String encodedUrl = null;
				try {
					encodedUrl = java.net.URLEncoder.encode(url,"UTF-8");
				} catch (UnsupportedEncodingException e1) {
					e1.printStackTrace();
				}
//				System.out.println("Got values for url: " + encodedUrl);
				String page = r.get("page");
				
//				System.out.println("Got page for url: " + url);

				
				KVSClient kvs = new KVSClient(kvsMasterPort);
					String urlDocId = null;
	
					try {
						if (kvs.existsRow(docIdTableName, encodedUrl)) {
//							System.out.println("Found doc here");
							
							Row row = kvs.getRow(docIdTableName, encodedUrl);
							urlDocId = row.get("mapping");
							
							try {
								
//								System.out.println("PAGE IS: " + page);
//								System.out.println("PAGE LENGTH IS :" + page.length());
								
								page = Jsoup.parse(page).text();
//								page = Jsoup.clean(page, Whitelist.none());
								page = page.replaceAll("[^\\x00-\\x7F]", " ");
								page = page.replaceAll("#", " ").replaceAll("\\p{Cc}", " ");
							} catch (Exception e1) {
								System.out.println("Could not clean html");
								e1.printStackTrace();
							}
							
//							System.out.println("Found mapping for the doc here");
							return urlDocId + "," + page;
						} 
	
					} catch (IOException e) {
						e.printStackTrace();
					}
					
					System.out.println("Did not find");
					
					return "";
			});
			
			
			long time3 = System.currentTimeMillis();
			
			System.out.println("URL Page Maping took: " + (time3-time2)/1000 + " seconds.");
			
			urlPageRDD.saveAsTable("urlPageRDD");
			
//			FlameRDD filteredRDD = urlPageRDD.filter(s -> {
//				if (s.equals("")) return false;
//				String[] urlPage = s.split(",");
//				String url = urlPage[0];
//				if (!url.contains("..")) {
//					return true;
//				} else {
//					return false;
//				}
//			});
			
//			filteredRDD.saveAsTable("filteredRDD");
			
			long time4 = System.currentTimeMillis();
			
//			System.out.println("Filtering took: " + (time3-time2)/1000 + " seconds.");

			FlamePairRDD urlPagePairRdd = urlPageRDD.mapToPair(s -> {
				int commaIndex = s.indexOf(",");
				String url = s.substring(0, commaIndex);
				String page = s.substring(commaIndex + 1);

				FlamePair fp = new FlamePair(url, page);
				return fp;
			});
			
			urlPagePairRdd.saveAsTable("urlPagePairRdd");

			long time5 = System.currentTimeMillis();
			
			System.out.println("URL, Page Mapping took: " + (time5-time4)/1000 + " seconds.");
			
			k.put(indexCountSoFarTable, "count", "count", String.valueOf(0));
			
			FlamePairRDD urlToDocMapping = urlPagePairRdd.flatMapToPair(p -> {
				String urlId = p._1();
				String pageContent = p._2();

				KVSClient kvs = new KVSClient(kvsMasterPort);
				
				Row countRow = kvs.getRow(indexCountSoFarTable, "count");
				String currcount = countRow.get("count");
				Integer curr = Integer.valueOf(currcount);
				curr = curr + 1;
				currcount = String.valueOf(curr);
				
				kvs.put(indexCountSoFarTable, "count", "count", currcount);
				
				System.out.println("urlId="+urlId);
				
				ArrayList<FlamePair> fp = new ArrayList<>();

				// ----------------------------------------------------------------------
				Map<String, Integer> countMap = new HashMap<>();
				// ----------------------------------------------------------------------
				
				// Processing
				pageContent = pageContent.replaceAll("\\<.*?\\>", " ");
				pageContent = pageContent.replaceAll("[.,:;!?'\"()-]", " ");
				pageContent = pageContent.replaceAll("[\r\n\t]", " ");
				pageContent = pageContent.toLowerCase();

				String[] words = pageContent.split("\\s+");				
				
				int wordPos = 1;
				
				for (String word : words) {
					if (!word.equals("")) {

						if (word.length()==5) {
							if (onlyDigits(word)) {
								if (word.compareTo("00501")>=0 && word.compareTo("99950")<=0) {
									if (kvs.existsRow("docToZip", urlId)) {
										Row r = kvs.getRow("docToZip", urlId);
										String currentZips = r.get("zipcode");
										String updatedZips = currentZips + "," + word;
										kvs.put("docToZip", urlId, "zipcode", updatedZips);
									} else {
										String zips = word;
										kvs.put("docToZip", urlId, "zipcode", zips);
									}
								}
							}
						}
						
						char[] wordChar = word.toCharArray();
						int len = wordChar.length;
						
						Stemmer stemmer = new Stemmer();
						stemmer.add(wordChar, len);
						stemmer.stem();
						String stemmedWord = stemmer.toString();

						// Add original word and the stemmed word to the index.
						
						if (countMap.containsKey(word)) {
							countMap.put(word, countMap.get(word)+1);
						} else {
							countMap.put(word, 1);
						}
						
						//addWordToIndex(word, urlId, indexTable, kvs, wordPos);
						
						/*if (!stemmedWord.equals(word)) {
							addWordToIndex(stemmedWord, urlId, indexTable, kvs, wordPos);
						}*/
						
						if (!stemmedWord.equals(word)) {
							if (countMap.containsKey(stemmedWord)) {
								countMap.put(stemmedWord, countMap.get(stemmedWord)+1);
							} else {
								countMap.put(stemmedWord, 1);
							}
						}
						
						wordPos = wordPos + 1;
						
					}
					
					/*if (countMap.size()>100) {
						batchProcessing(indexTable, countMap, kvs, urlId);
						countMap.clear();
						countMap = new HashMap<String, Integer>();
					}*/
					
					if (wordPos > 2000) {
						break;
					}
					
				}
				
				batchProcessing(indexTable, countMap, kvs, urlId);
				
				return fp;

			});
			
			long time6 = System.currentTimeMillis();
			
			System.out.println("Indexing took: " + (time6-time5)/1000 + " seconds.");

			// Compute inverse document frequency values for each word
			Iterator<Row> itr1 = k.scan(indexTable, null, null);
			while (itr1.hasNext()) {
				Row r = itr1.next();
				String word = r.key();
				String docList = r.get("doclist");
				String[] allDocuments = docList.split(",");

				int df = allDocuments.length;
				int docTotal = Integer
						.valueOf(new String(k.get(docCountSoFarTable, "count", "count"), StandardCharsets.UTF_8));

				double idfValue = (double) (docTotal) / (double) (df);
				double idf = Math.log10(idfValue);

				// Add document frequency and inverse document frequency back to the row.
				// Add the row back to the key value store.
				r.put("df", String.valueOf(df));
				r.put("idf", String.valueOf(idf));

				k.putRow(indexTable, r);
			}
			
			long time7 = System.currentTimeMillis();
			
			System.out.println("IDF took: " + (time7-time6)/1000 + " seconds.");

			// Ranking code ->
			
			/*
			// Ranking responses based on TF-IDF values.
			
			String[] queryStrings = { "Demosthenes Prince Andrew", "Bruenn's bloodthirsty"};

			for (String query : queryStrings) {
				Map<String, Double> response = ranker(k, indexTable, query);

				System.out.println("For query: " + query + " we got: ");
				for (String docId : response.keySet()) {
					System.out.println("DocID: " + docId + " Value: " + response.get(docId));
				}
				System.out.println("-------------------------------------");
				
				TreeMap<String, Double> finalRes = mergeRanks(k, response);
				for (String docId : finalRes.keySet()) {
					System.out.println("DocID: " + docId + " Value: " + finalRes.get(docId));
				}

				System.out.println("-------------------------------------");
			}
			
			// EC: Phrase Search 
			String[] exactMatchQueryStrings = {"Prince Andrew", "after their hard marches", "solemn affair", "gentlemen", "bloodthirsty soldier", "historic event", "commerce handicraft gardening", "Christ preached on the Cross"};
			
			for (String queryString: exactMatchQueryStrings) {
				List<String> matches = phraseSearch(k, indexTable, queryString);
				
				System.out.println("For query: " + queryString + " we got: ");
				for (String match: matches) {
					System.out.println("DocID: " + match);
				}
				System.out.println("-------------------------------------");
			}

			*/
	
			return;

		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}

	}

}
