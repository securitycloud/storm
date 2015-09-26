package cz.muni.fi.storm.tools.writers;

/**
 * Classes implements this interface able to send some messages and can be closed.
 */
public interface Writer {

    /**
     * Sends to write a message.
     * 
     * @param message
     */
    public void send(String message);

    /**
     * Closed this writer.
     */
    public void close();
}
