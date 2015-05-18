package cz.muni.fi.storm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import jline.internal.InputStreamReader;

public class FileFlowSource implements FlowSource {
    
    private File file;
    private BufferedReader reader = null;
    
    public FileFlowSource(File file) {
        if (file == null)
            throw new IllegalArgumentException("Provide file to be read" );

        this.file = file;

        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Source file for flows doesnt exist", e);
        }
    }
    
    @Override
    public String nextFlow() {
        if (closed()) {
            return null;
        }

        String line;

        try {
            line = reader.readLine();
        } catch (IOException e) {
            throw new RuntimeException("Error while reading from test file: " + file.getAbsolutePath(), e);
        }

        if (line == null) {
            close();
            return null;
        }

        return line;
    }
	
    private void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing file", e);
        }
        reader = null;
    }
    
    private boolean closed() {
        return reader == null;
    }

}
