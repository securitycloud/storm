package cz.muni.fi.storm.tools.writers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class FileWriter implements Writer {

    private File targetFile;
    private PrintWriter writer;

    public FileWriter(String filePath) {
        this.targetFile = new File(filePath);
        try {
            this.writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(targetFile, true)));
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Target file doesn't exist", e);
        }
    }

    @Override
    public void send(String message) {
        writer.println(message);
    }
    
    @Override
    public void close() {
        writer.close();
    }
}
