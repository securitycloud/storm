package cz.muni.fi.storm.tools.pojo;

import scala.Serializable;

/**
 * POJO for communication server - client and transmitted packets.
 */
public class ServerClient implements Serializable {
    
    private String server;
    private String client;
    private int sent;
    private int received;

    public String getServer() {
        return server;
    }
    public void setServer(String server) {
        this.server = server;
    }

    public String getClient() {
        return client;
    }
    public void setClient(String client) {
        this.client = client;
    }

    public int getSent() {
        return sent;
    }
    public void setSent(int sent) {
        this.sent = sent;
    }

    public int getReceived() {
        return received;
    }
    public void setReceived(int received) {
        this.received = received;
    }
}
