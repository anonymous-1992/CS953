package main.java.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class FileUtil {

    private static final Logger logger = LoggerFactory.getLogger(FileUtil.class);

    public static class StreamPair {
        public final Stream<String> output_stream, error_stream;
        public StreamPair(Stream<String> outputStream,Stream<String> errorStream) {
            output_stream = outputStream;
            error_stream = errorStream;
        }
    }

    /**
     * Utility method to open the train/test/score files.
     * @param dir Directory to store the file
     * @param filename Name of the file to write to
     * @return PrintWriter to write data to the file. Make sure you close it when you're done.
     */
    public static PrintWriter openOutputFile(String dir, String filename) {
        try {
            File trainOut = new File(dir);
            trainOut.mkdirs(); //In case directory does not exist.
            trainOut = new File(trainOut,filename);
            return new PrintWriter(
                    new FileOutputStream(trainOut, false));
        } catch(FileNotFoundException fnf) {
            throw new IllegalStateException("Unable to write training or test files: " + fnf.getMessage());
        }
    }

    /**
     * Returns a FileInputStream for the given file, or null if not found.
     */
    public static FileInputStream getFileInputStream(String file) {
        try {
            return new FileInputStream(new File(file));
        } catch(FileNotFoundException fnf) {
            System.err.println("Could not open input stream: " + fnf.getMessage());
        }
        return null;
    }

    /**
     * Returns a FileOutputStream for the given file, or null if not found.
     */
    public static FileOutputStream getFileOutputStream(String file) {
        try {
            Path fpath = Paths.get(file);
            Files.createDirectories(fpath.getParent()); //In case directory does not exist.
            return new FileOutputStream(new File(file));
        } catch(IOException fnf) {
            System.err.println("Could not open output stream: " + fnf.getMessage());
        }
        return null;
    }

    public static void makeDirectories(String [] dirs) {
        for(String dir : dirs) {
            try {new File(dir).mkdirs();}
            catch(SecurityException se) { logger.error("Failed to create directory: " + dir + ": " + se.getMessage());}
        }
    }
}
