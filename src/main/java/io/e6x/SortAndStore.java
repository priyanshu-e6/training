package io.e6x;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class SortAndStore {

    public static void main(String[] args) {
        final long len = 2L * 1024 * 1024 * 1024 / 4;
        long section_len = 2L * 1024 * 1024 * 1024;
        String[] files= new String[25];
        try (DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream("/home/priyanshu/Desktop/asgnone/inputFile")))
        ) {

            for (int i = 0; i < 25; i++) {
                int[] inputData = new int[(int) len];
                int j = 0;
                try {
                    while (true) {
                        inputData[j] = is.readInt();
                        j += 1;
                    }
                } catch (EOFException e) {

                }
                Arrays.sort(inputData);

                String file_name = "/home/priyanshu/Desktop/asgnone/inputFile" + (i+1);
                files[i] = file_name;
                try (FileOutputStream fileOutputStream = new FileOutputStream(file_name);
                     DataOutputStream os = new DataOutputStream(new BufferedOutputStream(fileOutputStream))) {
                    for (int number : inputData) {
                        os.writeInt(number);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
                e.printStackTrace();
        }
        mergeArraysWithHeaps(files);
    }
    private static void mergeArraysWithHeaps(String[] files) {

        PriorityQueue<ArrayElement> pq = new PriorityQueue<>();

        for (int i = 0; i < 25; i++) {
            try(DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(files[i])))){
                int val = dataInputStream.readInt();
                pq.offer(new ArrayElement(val, i+1));
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }

        while(!pq.isEmpty()) {
            ArrayElement minElement = pq.poll();
            int minVal = minElement.getValue();
            int chunkNumber = minElement.getChunkNumber();

            try(DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(files[chunkNumber])));
                DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("/home/priyanshu/Desktop/asgnone/outputFile")))){

                os.writeInt(minVal);
                int val = dataInputStream.readInt();
                pq.offer(new ArrayElement(val, chunkNumber));
            }
            catch(Exception e){
                e.printStackTrace();
            }

        }
    }
}
