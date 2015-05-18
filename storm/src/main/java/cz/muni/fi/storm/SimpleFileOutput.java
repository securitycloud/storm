package cz.muni.fi.storm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class SimpleFileOutput implements FileOutput {
    
    private File file = null;
	
    public SimpleFileOutput(String pathToOutput) {
        file = new File(pathToOutput);
    }

    @Override
    public void append(String string) {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
            pw.println(string);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error writing to output file", e);
        } finally {
            pw.close();
        }
    }

}
