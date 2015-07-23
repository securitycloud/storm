package cz.muni.fi.storm.tools.readers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import jline.internal.InputStreamReader;
import org.apache.commons.io.IOUtils;

public class FileReader implements Reader {

    private File fileSource;
    private BufferedReader reader;

    public FileReader(String filePath) {
        this.fileSource = new File(filePath);
        try {
            this.reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileSource)));
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Source file doesn't exist", e);
        }
    }
    
    @Override
    public String next() {
        if (reader == null) {
            return null;
        }

        String line;

        try {
            line = reader.readLine();
        } catch (IOException e) {
            throw new RuntimeException("Error while reading from file: " + fileSource.getAbsolutePath(), e);
        }

        if (line == null) {
            close();
            return null;
        }

        return line;
    }
    
    @Override
    public void close() {
        if (reader != null) {
            IOUtils.closeQuietly(reader);
        }
    }
}
