package cis5550.frontend;

// Java standard library imports
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;

// Third-party library imports
import org.json.simple.*;

// Project-specific imports
import cis5550.kvs.*;
import cis5550.tools.*;
import cis5550.webserver.Server;

import static cis5550.webserver.Server.*;

public class Master extends cis5550.generic.Master {

    private static final Logger logger = Logger.getLogger(Master.class);
    private static final String version = "v1 Apr 14 2023";
    public static KVSClient kvs;
    public static HashSet<String> stopWords = new HashSet<>(
            Arrays.asList("a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "he", "in", "is", "it",
                    "its", "of", "on", "that", "the", "to", "was", "were", "will", "with"));

    static ConcurrentHashMap<String, String> titleMap = new ConcurrentHashMap<String, String>();
    static ConcurrentHashMap<String, String> pageMap = new ConcurrentHashMap<String, String>();
    static ConcurrentHashMap<String, String> docIdMappingInMem = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        // Check valid command-line arguments
        if (args.length != 2) {
            System.err.println("Syntax: Master <port> <kvsMaster>");
            System.exit(1);
        }

        // Bind port and register routes
        int myPort = Integer.valueOf(args[0]);
        kvs = new KVSClient(args[1]);

        port(myPort);
        registerRoutes();

        String titleStorageDirectory = "worker2";

        FileInputStream tableFileIn = null;

        // KVStore.put(titleInMemoryMap, table);
        System.out.println("Creating title and page in memory map.");
        try {
            Path path = Paths.get(titleStorageDirectory, "titles.table");
            File file = new File(path.toString());
            tableFileIn = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException while reading the table file during server recovery");
            e.printStackTrace();
        }

        Row rowObj;
        try {
            while ((rowObj = Row.readFrom(tableFileIn)) != null) {
                String row = rowObj.key();
                // KVStore.get(firstTableName).put(row, rowObj);
                titleMap.put(row, rowObj.get("title"));
                String page = rowObj.get("page");
                page = page.substring(0, (int) Math.floor(page.length() / 4));
                pageMap.put(row, page);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Creating docIDmapping in memory map.");

        FileInputStream doctableFileIn = null;
        try {
            Path path = Paths.get(titleStorageDirectory, "docIdMapping.table");
            File file = new File(path.toString());
            doctableFileIn = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException while reading the table file during server recovery");
            e.printStackTrace();
        }

        // long time1 = System.currentTimeMillis();
        // Row rowObj;
        try {
            while ((rowObj = Row.readFrom(doctableFileIn)) != null) {
                String row = rowObj.key();
                // KVStore.get(firstTableName).put(row, rowObj);
                if (Character.isDigit(row.charAt(0))) {
                    docIdMappingInMem.put(row, rowObj.get("mapping"));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Loaded the docIdMappingTable");

        // Create a trie for autocomplete
        Trie autocompleteTrie = trieFromIndex();

        // -------------------------------------------------------------------------------------------------
        // NEW ROUTES
        // -------------------------------------------------------------------------------------------------
        get("/", (request, response) -> {
            response.type("text/html");
            return fileToString("assets_basic/index.html");
        });

        get("/resultsPage.html", (request, response) -> {
            response.type("text/html");
            return fileToString("assets_special/resultsPage.html");
        });

        get("/logoFront.svg", (request, response) -> {
            response.type("image/svg+xml");
            return fileToString("assets_special/logoFront.svg");
        });

        get("/logoSide.svg", (request, response) -> {
            response.type("image/svg+xml");
            return fileToString("assets_special/logoSide.svg");
        });

        get("/autocomplete.js", (request, response) -> {
            response.type("text/javascript");
            return fileToString("assets_special/autocomplete.js");
        });

        get("/searchResults.js", (request, response) -> {
            response.type("text/javascript");
            return fileToString("assets_special/searchResults.js");
        });

        get("/specialResults.js", (request, response) -> {
            response.type("text/javascript");
            return fileToString("assets_special/specialResults.js");
        });

        get("/styles.css", (request, response) -> {
            response.type("text/css");
            return fileToString("assets_special/styles.css");
        });

        get("/homePageStyle.css", (request, response) -> {
            response.type("text/css");
            return fileToString("assets_special/homePageStyle.css");
        });

        get("/fetchAutocompleteResults", (request, response) -> {
            String userInput = request.queryParams("userInput");
            int lastSpacePosition = userInput.lastIndexOf(" ");
            List<String> autocompleteResults = autocompleteTrie
                    .autoComplete(userInput.substring(lastSpacePosition + 1));

            // Put autocomplete results into JSON array
            JSONArray jsonArray = new JSONArray();
            for (int i = 0; i < 10 && i < autocompleteResults.size(); i++) {
                jsonArray.add(autocompleteResults.get(i));
            }

            response.type("application/json");
            // Send back the JSON object
            return jsonArray;
        });

        /*
         * Returns the urls most relevant to this link
         */
        get("/fetchResults", (request, response) -> {
            JSONArray resultsArray = new JSONArray();

            String query = request.queryParams("userInput");
            String[] queryWords = splitQueryString(query);
            Long time1 = System.currentTimeMillis();
            ConcurrentHashMap<String, Double> ranksUnsorted = ranker(kvs, "index", queryWords);
            Long time2 = System.currentTimeMillis();
            System.out.println("Ranker time: " + String.valueOf((time2 - time1) / 1000));

            // Sort URLs by rank score.
            time1 = System.currentTimeMillis();
            List<Map.Entry<String, Double>> ranksSorted = new LinkedList<Map.Entry<String, Double>>(
                    ranksUnsorted.entrySet());
            Collections.sort(ranksSorted, new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> o1,
                        Map.Entry<String, Double> o2) {
                    return (o2.getValue()).compareTo(o1.getValue());
                }
            });
            time2 = System.currentTimeMillis();
            System.out.println("Sorting time: " + String.valueOf((time2 - time1) / 1000));

            // Package data to JSON format
            time1 = System.currentTimeMillis();
            int counter = 0;
            for (Map.Entry<String, Double> pair : ranksSorted) {
                JSONObject result = new JSONObject();
                // String url_raw = docIdMappingInMem.get(pair.getKey());
                // String url = java.net.URLDecoder.decode(url_raw, "UTF-8"); // Boost url's
                // with search terms
                byte[] url_raw = kvs.get("docIdMapping", pair.getKey(), "mapping");
                String url_encoded = new String(url_raw, StandardCharsets.UTF_8);
                String url = java.net.URLDecoder.decode(url_encoded, "UTF-8"); // Boost url's with search terms

                // byte[] page_raw = kvs.get("wiki202142-batch1", Hasher.hash(url), "page");
                // byte[] title_raw = kvs.get("wiki202142-batch1", Hasher.hash(url), "title");

                // String page;
                // String description;
                // if (page_raw == null) {
                // page = "No match in crawl table";
                // description = "No match in crawl table";
                // } else {
                // page = new String(page_raw, StandardCharsets.UTF_8);
                // description = page.substring(0, 200);
                // }

                String title = titleMap.get(Hasher.hash(url));
                if (title == null) {
                    title = "No title found";
                }

                String page = pageMap.get(Hasher.hash(url));
                if (page == null) {
                    page = "No page found";
                }

                result.put("title", title);
                result.put("description", page);
                result.put("url", url);
                resultsArray.add(result);
                if (counter == 100) {
                    break;
                }
                counter++;
            }
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("results", resultsArray);
            time2 = System.currentTimeMillis();
            System.out.println("Packing JSON time: " + String.valueOf((time2 - time1) / 1000));

            // Send search results to client
            response.type("application/json");

            System.out.println("END OF FETCH RESULTS");
            return jsonObject;
        });
    }

    // -------------------------------------------------------------------------------------------------
    // HELPER FUNCTIONS
    // -------------------------------------------------------------------------------------------------

    /**
     * Reads the contents of a file into a String.
     *
     * @param pathToFile the path to the file to read
     * @return the contents of the file as a String
     */
    public static String fileToString(String pathToFile) {
        Path filePath = Path.of(pathToFile);
        String toReturn;
        try {
            // Read the contents of the file into a String
            toReturn = Files.readString(filePath);
        } catch (IOException e) {
            // If the file cannot be read, return a 404 error message
            toReturn = "404 Not Found";
        }
        return toReturn;
    }

    public static String[] splitQueryString(String queryString) {
        queryString = queryString.replaceAll("\\<.*?\\>", " ");
        queryString = queryString.replaceAll("[.,:;!?'\"()-]", " ");
        queryString = queryString.replaceAll("[\r\n\t]", " ");
        queryString = queryString.toLowerCase();
        String[] words = queryString.split("\\s+");
        List<String> cleanedWords = new ArrayList<>();
        for (String word : words) {
            if (!stopWords.contains(word)) {
                cleanedWords.add(word);
            }
        }
        return cleanedWords.toArray(new String[0]);
    }

    public static void loadPenn(JSONArray array) {
        JSONObject result = new JSONObject();
        // String url_raw = docIdMappingInMem.get(pair.getKey());
        // String url = java.net.URLDecoder.decode(url_raw, "UTF-8"); // Boost url's
        // with search terms
        byte[] url_raw = kvs.get("docIdMapping", pair.getKey(), "mapping");
        String url_encoded = new String(url_raw, StandardCharsets.UTF_8);
        String url = java.net.URLDecoder.decode(url_encoded, "UTF-8"); // Boost url's with search terms

        // byte[] page_raw = kvs.get("wiki202142-batch1", Hasher.hash(url), "page");
        // byte[] title_raw = kvs.get("wiki202142-batch1", Hasher.hash(url), "title");

        // String page;
        // String description;
        // if (page_raw == null) {
        // page = "No match in crawl table";
        // description = "No match in crawl table";
        // } else {
        // page = new String(page_raw, StandardCharsets.UTF_8);
        // description = page.substring(0, 200);
        // }

        String title = titleMap.get(Hasher.hash(url));
        if (title == null) {
            title = "No title found";
        }

        String page = pageMap.get(Hasher.hash(url));
        if (page == null) {
            page = "No page found";
        }

        result.put("title", title);
        result.put("description", page);
        result.put("url", url);
        resultsArray.add(result);
        if (counter == 100) {
            break;
        }
        counter++;
    }

    public static Set<String> sampleDocuments(Set<String> allDocuments, int maxSize) {
        if (allDocuments.size() < maxSize) {
            return allDocuments;
        }

        int shrinkFactor = Math.floorDiv(allDocuments.size(), maxSize);

        Set<String> sampleDocuments = new HashSet<>();
        for (String doc : allDocuments) {
            if (Double.parseDouble(doc) % shrinkFactor == 0) {
                sampleDocuments.add(doc);
            }
        }
        return sampleDocuments;
    }

    public static Set<String> sampleDocuments_2(Set<String> allDocuments, int maxSize) {
        int n = allDocuments.size();
        if (n > maxSize) {
            List<String> allDocumentsList = new ArrayList<String>(allDocuments);

            // allDocuments.clear();
            Set<String> newSet = new HashSet<>();

            while (newSet.size() < maxSize) {
                int randomIndex = (int) Math.floor(Math.random() * (n));

                newSet.add(allDocumentsList.get(randomIndex));
            }

            return newSet;
        }
        return allDocuments;
    }

    public static void updateRankingsForWordWithBoost(KVSClient k, String indexTable, String word,

            ConcurrentHashMap<String, Double> rankings, long N) {

        Long time1 = System.currentTimeMillis();
        try {

            if (k.existsRow(indexTable, word)) {

                // System.out.println("Found word in index: " + word);

                Row r = k.getRow(indexTable, word);

                Set<String> allDocuments = r.columns();
                // System.out.println("Before sampling: " + allDocuments.size());
                // Set<String> sampleDocuments = sampleDocuments(allDocuments, 10000);
                Set<String> sampleDocuments = sampleDocuments_2(allDocuments, 2000);
                // System.out.println("After sampling: " + sampleDocuments.size());

                int n = sampleDocuments.size();

                // System.out.println("Count of documents with word: " + String.valueOf(n));

                double idf = Math.log10((double) (N) / (double) (n));

                for (String doc : sampleDocuments) {

                    String url_raw = docIdMappingInMem.get(doc);
                    String url;
                    if (url_raw == null) {
                        // System.out.println("--------No url mapping found : " + doc);
                        continue;
                    } else {
                        url = java.net.URLDecoder.decode(url_raw, "UTF-8"); // Boost url's with search terms
                    }

                    // byte[] url_raw = k.get("docIdMapping", doc, "mapping");

                    // String url_encoded = new String(url_raw, StandardCharsets.UTF_8);

                    // String url = java.net.URLDecoder.decode(url_encoded, "UTF-8"); // Boost url's
                    // with search terms

                    // byte[] title_raw = k.get("wiki202142-batch1", Hasher.hash(url), "title");

                    // String title = new String(title_raw, StandardCharsets.UTF_8);

                    String title = titleMap.get(Hasher.hash(url));

                    String termFrequency = r.get(doc);

                    Double docWeight = (1 + Math.log10(Double.parseDouble(termFrequency))) * idf;

                    if (rankings.containsKey(doc)) {

                        synchronized (rankings) {
                            double currValueForDoc = rankings.get(doc);

                            System.out.println("For url: " + url + " before update: " + currValueForDoc);

                            double updatedValueForDoc = currValueForDoc + docWeight;

                            if (url.toLowerCase().contains(word)) {

                                updatedValueForDoc *= 50;

                            }

                            System.out.println("For url: " + url + " docWeight: " + docWeight + " updatedValueForDoc: "
                                    + updatedValueForDoc);

                            // if (title != null && title.toLowerCase().contains(word)) {

                            // updatedValueForDoc *= 5;

                            // }

                            rankings.put(doc, updatedValueForDoc);
                        }
                    } else {

                        synchronized (rankings) {
                            if (url.toLowerCase().contains(word)) {

                                docWeight *= 50;

                            }

                            // if (title != null && title.toLowerCase().contains(word)) {

                            // docWeight *= 5;

                            // }

                            rankings.put(doc, docWeight);
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

            // System.out.println("Stemmed word for " + word + ": " + stemmedWord);

            if (k.existsRow(indexTable, stemmedWord) && !stemmedWord.equals(word)) {

                // System.out.println("Found stemmed word in index: " + stemmedWord);

                Row r = k.getRow(indexTable, stemmedWord);

                Set<String> allDocuments = r.columns();
                // System.out.println("Before sampling: " + allDocuments.size());
                // Set<String> sampleDocuments = sampleDocuments(allDocuments, 10000);
                Set<String> sampleDocuments = sampleDocuments_2(allDocuments, 2000);
                // System.out.println("After sampling: " + sampleDocuments.size());

                int n = sampleDocuments.size();

                // System.out.println("Count of documents with word: " + String.valueOf(n));

                double idf = Math.log10((double) (N) / (double) (n));

                for (String doc : sampleDocuments) {
                    // String url_raw = docIdMappingInMem.get(doc);
                    // String url = java.net.URLDecoder.decode(url_raw, "UTF-8"); // Boost url's
                    // with search terms

                    String url_raw = docIdMappingInMem.get(doc);
                    String url;
                    if (url_raw == null) {
                        // System.out.println("--------No url mapping found : " + doc);
                        continue;
                    } else {
                        url = java.net.URLDecoder.decode(url_raw, "UTF-8"); // Boost url's with search terms
                    }

                    // byte[] url_raw = k.get("docIdMapping", doc, "mapping");

                    // String url_encoded = new String(url_raw, StandardCharsets.UTF_8);

                    // String url = java.net.URLDecoder.decode(url_encoded, "UTF-8"); // Boost url's
                    // with search terms

                    // byte[] title_raw = k.get("wiki202142-batch1", Hasher.hash(url), "title");

                    // String title = new String(title_raw, StandardCharsets.UTF_8);

                    String title = titleMap.get(Hasher.hash(url));

                    String termFrequency = r.get(doc);

                    Double docWeight = (1 + Math.log10(Double.parseDouble(termFrequency))) * idf;

                    if (rankings.containsKey(doc)) {

                        synchronized (rankings) {
                            double currValueForDoc = rankings.get(doc);

                            System.out.println("[STEMMING] For url: " + url + " before update: " + currValueForDoc);

                            double updatedValueForDoc = currValueForDoc + docWeight;

                            if (url.toLowerCase().contains(word)) {

                                updatedValueForDoc *= 25;

                            }

                            // if (title != null && title.toLowerCase().contains(word)) {

                            // updatedValueForDoc *= 5;

                            // }

                            System.out.println("[STEMMING] For url: " + url + " docWeight: " + docWeight
                                    + " updatedValueForDoc: " + updatedValueForDoc);

                            rankings.put(doc, updatedValueForDoc);
                        }
                    } else {

                        synchronized (rankings) {
                            if (url.toLowerCase().contains(word)) {

                                docWeight *= 25;

                            }

                            // if (title != null && title.toLowerCase().contains(word)) {

                            // docWeight *= 5;

                            // }

                            rankings.put(doc, docWeight);
                        }

                    }

                }

            }

            System.out.println("-----------------------------------------------------");

        } catch (NumberFormatException | IOException e) {
            e.printStackTrace();
        }
        Long time2 = System.currentTimeMillis();
        // System.out.println("Boost time for " + word + ": " + String.valueOf((time2 -
        // time1) / 1000));

        return;
    }

    public static ConcurrentHashMap<String, Double> ranker(KVSClient k, String indexTable, String[] queryStringSplit) {

        int len = queryStringSplit.length;

        Thread threads[] = new Thread[len];

        ConcurrentHashMap<String, Double> rankings = new ConcurrentHashMap<>();

        try {

            for (int i = 0; i < len; i++) {

                String word = queryStringSplit[i];

                threads[i] = new Thread("Process word" + (i + 1)) {

                    public void run() {

                        long N;

                        // N = k.count("docIdMapping") / 2;
                        N = docIdMappingInMem.size() / 2;
                        updateRankingsForWordWithBoost(k, indexTable, word, rankings, N);

                    }

                };

                threads[i].start();

            }

        } catch (NumberFormatException e) {

            e.printStackTrace();

        }

        for (int i = 0; i < threads.length; i++) {

            try {

                threads[i].join();

            } catch (InterruptedException ie) {

            }

        }

        return rankings;

    }

    public static Trie trieFromIndex() {
        Trie toReturn = new Trie();

        String fileName = "src/cis5550/frontend/words_alpha.txt";
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                toReturn.insert(line);
            }
        } catch (IOException e) {
            return new Trie();
        }

        return toReturn;
    }
}
