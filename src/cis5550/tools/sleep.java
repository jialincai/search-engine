package cis5550.tools;

public class sleep {
    public static void main(String[] args) {
        System.out.println("Sleeping for 10 seconds...");
        try {
            Thread.sleep(10000); // sleep for 10 seconds (10,000 milliseconds)
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Done sleeping.");
    }
}