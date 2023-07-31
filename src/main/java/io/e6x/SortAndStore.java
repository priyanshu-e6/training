package io.e6x;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class SortAndStore {

    public static void main(String[] args) {
        final long len = 2L * 1024 / 4;
        long file_size = len * 4;
        String[] files = new String[25];
        long time_makeChunks = System.currentTimeMillis();
        try (DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream("/home/priyanshu/Desktop/asgnone/inputFile")))
        ) {
            System.out.println("Sorting into chunks started");
            for (int i = 0; i < 25; i++) {
                int[] inputData = new int[(int) len];

                for (int j = 0; j < len; j++) {
                    inputData[j] = is.readInt();
                }

                Arrays.sort(inputData);

                String file_name = "/home/priyanshu/Desktop/asgnone/inputFile" + (i + 1);
                files[i] = file_name;
                try (FileOutputStream fileOutputStream = new FileOutputStream(file_name);
                     DataOutputStream os = new DataOutputStream(new BufferedOutputStream(fileOutputStream))) {

                    int inputDataLength = inputData.length;

                    for (int j = 0; j < inputDataLength; j++) {
                        int number = inputData[j];
                        os.writeInt(number);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        long timeElapse_makeChunks = System.currentTimeMillis() - time_makeChunks;
        System.out.println("Time elapsed to make sorted chunks: " + timeElapse_makeChunks);

        System.out.println("merging of chunks started");
        long time_mergeChunks = System.currentTimeMillis();
        mergeArraysWithHeaps(files);
        long elapsedTime_mergeChunks = System.currentTimeMillis() - time_makeChunks;
        System.out.println("Time passed to merge chunks: " + time_mergeChunks);
    }

    private static void mergeArraysWithHeaps(String[] files) {
        PriorityQueue<ArrayElement> pq = new PriorityQueue<>();
        int numPolls = 0;

        List<DataInputStream> inputStreams = new ArrayList<>();
        for(int i=0;i<25;i++){
            try {
                inputStreams.add(new DataInputStream(new BufferedInputStream(new FileInputStream(files[i]))));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        for (int i = 0; i < 25; i++) {
            try {
                DataInputStream dis = inputStreams.get(i);
                int val = dis.readInt();
                pq.offer(new ArrayElement(val, i));
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        }

        while (!pq.isEmpty()) {

            ArrayElement minElement = pq.poll();
            numPolls += 1;
            int minVal = minElement.getValue();
            int chunkNumber = minElement.getChunkNumber();

            try (
                    DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("/home/priyanshu/Desktop/asgnone/outputFile")))) {
                os.writeInt(minVal);
                DataInputStream dis = inputStreams.get(chunkNumber);

                try {
                    int val = dis.readInt();
                    pq.offer(new ArrayElement(val, chunkNumber));
                }
                catch(EOFException e){

                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("Number of polls: " + numPolls);
    }

    private static void SortArray(int[] toSort){

    }
}
