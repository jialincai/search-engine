package cis5550.generic;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Worker{
	
	public static String MY_ID = "";
	
	public static Runnable pingImpl(int workerServerPort, String storageDirectory, String ipPort){
		
	    Runnable helperRunnable = new Runnable(){
			public void run() {
				String workerID = "";

				String slash = "";
				if (storageDirectory.charAt(storageDirectory.length() - 1) != '/')
					slash = "/";

				Path idPath = Paths.get(storageDirectory + slash + "id");

				if (Files.exists(idPath)) {
					try {
						workerID = Files.readString(idPath);
					} catch (IOException e1) {
						System.out.println("IOException while reading id file");
						e1.printStackTrace();
					}
				} else {
					Random random = new Random();
					workerID = random.ints(97, 122 + 1) // 97 - a; 122 - z
							.limit(5) // id length
							.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
							.toString();
					try {
						Files.createDirectories(Paths.get(storageDirectory));
						Files.writeString(idPath, workerID, StandardCharsets.UTF_8);
					} catch (IOException e) {
						System.out.println("IOException while writing to id file");
						e.printStackTrace();
					}
					
				}
				
				MY_ID = workerID;
				System.out.println("id: "+workerID);

				String URLlink = String.format("http://%s/ping?id=%s&port=%s", ipPort, workerID, workerServerPort);
				URL url1;

				while (true) {

					try {
						url1 = new URL(URLlink);
						url1.getContent();
						Thread.sleep(5000);
					} catch (MalformedURLException e) {
						System.out.println("MalformedURLException while creating a URL for worker");
						e.printStackTrace();
					} catch (IOException e) {
						System.out.println("IOException while triggering HTTP request for worker");
						e.printStackTrace();
					} catch (InterruptedException e) {
						System.out.println("InterruptedException at thread sleep in pingImpl");
						e.printStackTrace();
					}

				}
			}
	    };

	    return helperRunnable;

	}

    
	public static void startPingThread(int workerServerPort, String storageDirectory, String ipPort) {
		
		Thread thread = new Thread(pingImpl(workerServerPort, storageDirectory, ipPort));
        thread.start();
        	
	}
	
}