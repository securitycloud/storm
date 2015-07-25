package cz.muni.fi.storm.tools.readers;

public interface Reader {
    
    public String next();
    
    public void close();
}
