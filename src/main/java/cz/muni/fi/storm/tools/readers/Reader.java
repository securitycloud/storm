package cz.muni.fi.storm.tools.readers;

public interface Reader extends AutoCloseable {
    
    public String next();
    
    @Override
    public void close();
}
