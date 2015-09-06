package org.saliya.memmapcomm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

public class GenerateRandomPoints {
    public static void main(String[] args) {
        String file = args[0];
        int numPoints = Integer.parseInt(args[1]);

        try(BufferedWriter bw = Files.newBufferedWriter(Paths.get(file))) {
            PrintWriter writer = new PrintWriter(bw, true);
            for (int i = 0; i < numPoints; ++i){
                writer.println(Math.random());
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
