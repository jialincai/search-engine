package cis5550.webserver;

public interface Constants {
    public enum ModifiedState {
        TRUE, FALSE, ERR
    };

    public static final int MAX_CONNECTION_WORKERS = 100;
    public static final String CRLF = "\r\n";
    public static final int EOF = -1;
    public static final int defaultServerPort = 80;
}
