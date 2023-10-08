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

public class IndexerProject {

	public static String CRAWLTABLENAME = "wiki202142-batch7";
	public static String docIdTableName = "docIdMapping";
	public static String docCountSoFarTable = "docCount";
	public static String indexTable = "index";
	public static String indexCountSoFarTable = "indexCount";

	public static void main(String[] args) {

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

			String seperator = "~`~";

			int ptr = 1;

			for (String word : countMap.keySet()) {

				Integer count = countMap.get(word);
				ptr = ptr + 1;

				if (kvs.existsRow(indexTable, word)) {

					Row wordRow = kvs.getRow(indexTable, word);

					if (wordRow.get(urlId) == null) {

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
							valueString = valueString + String.valueOf(count);
						} else {
							valueString = valueString + seperator + String.valueOf(count);
						}

						// Add document in wordlist

						/*
						 * String currentDocList = wordRow.get("doclist"); String updatedDocList =
						 * currentDocList + "," + urlId;
						 * 
						 * rowString = rowString + seperator + word; colString = colString + seperator +
						 * "doclist"; valueString = valueString + seperator + updatedDocList;
						 */

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
						valueString = valueString + String.valueOf(count);
					} else {
						valueString = valueString + seperator + String.valueOf(count);
					}

					/*
					 * rowString = rowString + seperator + word; colString = colString + seperator +
					 * "doclist"; valueString = valueString + seperator + urlId;
					 */
				}

				if (ptr > 100) {
					kvs.batchPut(indexTable, rowString, colString, valueString);
					ptr = 1;
					rowString = "";
					colString = "";
					valueString = "";
				}

			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static boolean onlyDigits(String zipString) {
		String regex = "[0-9]+";
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(zipString);
		return m.matches();
	}

	static boolean validWord(String word) {

		char ch = word.charAt(0);
		if ((Character.compare(ch, '#') == 0 || Character.compare(ch, '$') == 0 || Character.compare(ch, '%') == 0)
				&& word.length() == 1) {
			return false;
		} else if (Character.isDigit(ch) || Character.isAlphabetic(ch) || Character.compare(ch, '$') == 0
				|| Character.compare(ch, '#') == 0 || Character.compare(ch, '%') == 0) {
			return true;
		} else {
			return false;
		}

	}

	public static void run(FlameContext fc, String[] args) {

		try {
			
			CRAWLTABLENAME = args[0];
			String startCount = args[1];
			
			KVSClient k = fc.getKVS();
			String kvsMasterPort = fc.getKVS().getMaster();

			if (!k.existsRow(docCountSoFarTable, "count")) {
				k.put(docCountSoFarTable, "count", "count", startCount);
			}

			// k.delete(indexTable);
			// k.delete(docIdTableName);

			long time1 = System.currentTimeMillis();

			Iterator<Row> itr = k.scan(CRAWLTABLENAME, null, null);

			while (itr.hasNext()) {

				Row r = itr.next();
				String url = r.get("url");
				String encodedUrl = java.net.URLEncoder.encode(url, "UTF-8");

				String urlDocId = null;

				if (!k.existsRow(docIdTableName, encodedUrl) && !encodedUrl.contains("..")) {

					Row countRow = k.getRow(docCountSoFarTable, "count");
					urlDocId = countRow.get("count");
					Integer idSoFar = Integer.valueOf(urlDocId);
					idSoFar = idSoFar + 1;
					urlDocId = String.valueOf(idSoFar);

					k.put(docCountSoFarTable, "count", "count", urlDocId);

					k.put(docIdTableName, urlDocId, "mapping", encodedUrl);
					k.put(docIdTableName, encodedUrl, "mapping", urlDocId);

				}

			}

			long time2 = System.currentTimeMillis();

			System.out.println("Doc ID Mapping took: " + (time2 - time1) / 1000 + " seconds.");

			Thread.sleep(5000);
			time2 = System.currentTimeMillis();

			FlamePairRDD urlPagePairRdd = fc.fromTable(CRAWLTABLENAME, r -> {

				String url = r.get("url");
				String encodedUrl = null;
				try {
					encodedUrl = java.net.URLEncoder.encode(url, "UTF-8");
				} catch (UnsupportedEncodingException e1) {
					e1.printStackTrace();
				}

				KVSClient kvs = new KVSClient(kvsMasterPort);
				String urlDocId = null;

				try {
					if (kvs.existsRow(docIdTableName, encodedUrl)) {

						Row row = kvs.getRow(docIdTableName, encodedUrl);

						String page = r.get("page");
						
						if (page != null && !page.equals("") && page.length() > 30000)
							page = page.substring(0, 30000);
						
						urlDocId = row.get("mapping");

						try {
							page = Jsoup.parse(page).text();
							page = page.replaceAll("[^\\x00-\\x7F]", " ");
							page = page.replaceAll("#", " ").replaceAll("\\p{Cc}", " ");
						} catch (Exception e1) {
							System.out.println("Could not clean html");
							e1.printStackTrace();
						}

						return urlDocId + "," + page;
					}

				} catch (IOException e) {
					e.printStackTrace();
				}

				System.out.println("Did not find");
				return "";
			}).filter(s -> {
				if (s.equals(""))
					return false;
				else
					return true;
			}).mapToPair(s -> {
				int commaIndex = s.indexOf(",");
				String url = s.substring(0, commaIndex);
				String page = s.substring(commaIndex + 1);

				FlamePair fp = new FlamePair(url, page);
				return fp;
			});

			long time3 = System.currentTimeMillis();

//			System.out.println("URL Page Maping took: " + (time3 - time2) / 1000 + " seconds.");

//			urlPageRDD.saveAsTable("urlPageRDD");

			long time4 = System.currentTimeMillis();

//			FlamePairRDD urlPagePairRdd = urlPageRDD.mapToPair(s -> {
//				int commaIndex = s.indexOf(",");
//				String url = s.substring(0, commaIndex);
//				String page = s.substring(commaIndex + 1);
//
//				FlamePair fp = new FlamePair(url, page);
//				return fp;
//			});

			urlPagePairRdd.saveAsTable("urlPagePairRdd");

			long time5 = System.currentTimeMillis();

			System.out.println("URL, Page Mapping took: " + (time5 - time3) / 1000 + " seconds.");

			k.put(indexCountSoFarTable, "count", "count", String.valueOf(0));
			
			k.persist(indexTable);

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

				System.out.println("urlId=" + urlId);

				ArrayList<FlamePair> fp = new ArrayList<>();

				Map<String, Integer> countMap = new HashMap<>();

				// Processing
				pageContent = pageContent.replaceAll("\\<.*?\\>", " ");
				pageContent = pageContent.replaceAll("[.,:;!?'\"()%-@#&*^`<>(){}]", " ").replaceAll("\\[", " ")
						.replaceAll("\\]", " ");
				pageContent = pageContent.replaceAll("[\r\n\t]", " ");
				pageContent = pageContent.toLowerCase();

				String[] words = pageContent.split("\\s+");

				int wordPos = 1;

				for (String word : words) {
					if (!word.equals("") && validWord(word)) {

						if (word.length() == 5) {
							if (onlyDigits(word)) {
								if (word.compareTo("00501") >= 0 && word.compareTo("99950") <= 0) {
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
							countMap.put(word, countMap.get(word) + 1);
						} else {
							countMap.put(word, 1);
						}

						if (!stemmedWord.equals(word)) {
							if (countMap.containsKey(stemmedWord)) {
								countMap.put(stemmedWord, countMap.get(stemmedWord) + 1);
							} else {
								countMap.put(stemmedWord, 1);
							}
						}

						wordPos = wordPos + 1;

					}

					/*
					 * if (countMap.size() > 100) { batchProcessing(indexTable, countMap, kvs,
					 * urlId); countMap.clear(); countMap = new HashMap<String, Integer>(); }
					 */

					if (wordPos > 1500) {
						break;
					}

				}

				batchProcessing(indexTable, countMap, kvs, urlId);

				return fp;

			});

			long time6 = System.currentTimeMillis();

			System.out.println("Indexing took: " + (time6 - time5) / 1000 + " seconds.");

			// Compute document frequency

			/*
			 * Iterator<Row> itr1 = k.scan(indexTable, null, null); while (itr1.hasNext()) {
			 * 
			 * Row r = itr1.next(); String word = r.key(); Set<String> cols = r.columns();
			 * 
			 * int df = cols.size(); r.put("df", String.valueOf(df));
			 * 
			 * k.putRow(indexTable, r); }
			 * 
			 * long time7 = System.currentTimeMillis();
			 * 
			 * System.out.println("DF took: " + (time7 - time6) / 1000 + " seconds.");
			 */

			return;

		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}

	}

}
