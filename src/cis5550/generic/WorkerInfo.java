package cis5550.generic;

public class WorkerInfo {

    protected String id_;
    protected String ip_;
    protected int portNumber_;

    protected long lastPing_;

    public WorkerInfo(String id, String ip, int portNumber) {
        id_ = id;
        ip_ = ip;
        portNumber_ = portNumber;
        lastPing_ = System.currentTimeMillis();
    }
}
