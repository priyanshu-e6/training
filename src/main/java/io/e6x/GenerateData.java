package io.e6x;

import java.io.*;
import java.util.Random;

public class GenerateData {
    public static void main(String[] args) {

        //generate random numbers file
        final long startTime = System.nanoTime();
        Random random = new Random();
        int numsPut = 0;
        try (DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("/home/priyanshu/Desktop/asgnone/inputFile")))) {
            //OutputStreamWriter converts the integer data into bytes and writes in a file
            // Generate and store random integers in the file
            final long totalNumbers = 2*1024*1024*1024L >> 2;

            for (int i = 0; i < totalNumbers; i++) {
                int randomNumber = random.nextInt();
                //System.out.print(Integer.toString(randomNumber) + ", ");
                os.writeInt(randomNumber);
                numsPut += 1;
                // TODO: possible speedup
            }
            os.flush();
            System.out.println("Random integers have been written to the file.");
            final long duration = (System.nanoTime() - startTime) / 1_000_000;
            System.out.println("Time taken to generate random numbers and write into a file: " + duration);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}