package cz.muni.fi.storm.tools.writers;

public interface Writer {

    public void send(String message);

    public void close();
}
