package io.e6x;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

public class GenerateData {

    public static void main(String[] args) {

        //generate random numbers file
        final long startTime = System.nanoTime();
        Random random = new Random();
        int buffersize = 1024*1024;
        try (BufferedWriter os = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/priyanshu/Desktop/asgnone/inputFile")), buffersize)) {
            //OutputStreamWriter converts the integer data into bytes and writes in a file
            // Generate and store random integers in the file
            int totalnumbers = 2*1024*1024*1024/4;

            for (int i = 0; i < 48; i++) {
                int randomNumber = random.nextInt();
                //System.out.print(Integer.toString(randomNumber) + ", ");
                os.write(randomNumber);

            }
            os.flush();
            System.out.println("Random integers have been written to the file.");
            final long duration = System.nanoTime() - startTime;
            System.out.println(duration);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}