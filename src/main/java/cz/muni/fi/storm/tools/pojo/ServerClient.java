package cz.muni.fi.storm.tools.pojo;

import scala.Serializable;

/**
 * POJO for communication server - client and transmitted packets.
 */
public class ServerClient implements Serializable {
    
    private String server;
    private String client;
    private int port;
    private int protocol;
    private int sent;
    private int received;

    public String getServer() {
        return server;
    }
    public void setLongServer(String server) {
        String[] part = server.split("/");
        this.server = part[0];
        this.port = new Integer(part[1]);
        this.protocol = new Integer(part[2]);
    }

    public String getClient() {
        return client;
    }
    public void setClient(String client) {
        this.client = client;
    }

    public int getPort() {
        return port;
    }

    public int getProtocol() {
        return protocol;
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
