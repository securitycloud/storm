package cz.muni.fi.storm.tools.writers;

public interface Writer extends AutoCloseable {

    public void send(String message);

    @Override
    public void close();
}
