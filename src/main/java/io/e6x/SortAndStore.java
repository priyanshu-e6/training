package io.e6x;
import java.io.*;
import java.util.*;

import java.util.concurrent.*;

public class SortAndStore {

    public static void main(String[] args) throws IOException{
        final long len = 2L * 1024 * 1024 *  1024 / 4;
        ExecutorService executorServiceToSort = Executors.newFixedThreadPool(8);
        long time_makeChunks = System.currentTimeMillis();
        int totalFiles = 20;
        String[] files = new String[totalFiles];

        try (DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream("/home/priyanshu/Desktop/asgnone/inputFile")))
        ) {
            System.out.println("Sorting into chunks started");
            for (int i = 0; i < totalFiles; i++) {

                int[] inputData = new int[(int) len];
                for (int j = 0; j < len; j++) {
                    inputData[j] = is.readInt();
                }
                long timeSortEachChunk = System.currentTimeMillis();

                String fileName = "/home/priyanshu/Desktop/asgnone/inputFile" + (i + 1);
                files[i] = fileName;

                SortArrayAndStore(inputData, (int)len, executorServiceToSort, fileName);
                long timeElapse_sortEachChunk = System.currentTimeMillis() - timeSortEachChunk;
                //System.out.println("time to sort chunk and write " + i+1 + " with normal sorting " + ": " + timeElapse_sortEachChunk);
                System.out.println("time to sort chunk " +  i + " with parallel sorting" + ": " + timeElapse_sortEachChunk);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        executorServiceToSort.shutdown();

        long timeElapse_makeChunks = System.currentTimeMillis() - time_makeChunks;
        System.out.println("Time elapsed to make sorted chunks: " + timeElapse_makeChunks/1_000);

        System.out.println("merging of chunks started");
        long time_mergeChunks = System.currentTimeMillis();
        mergeArraysWithHeaps(files, totalFiles);
        long elapsedTime_mergeChunks = System.currentTimeMillis() - time_mergeChunks;
        System.out.println("Time passed to merge chunks: " + elapsedTime_mergeChunks);
        long totalTime = System.currentTimeMillis() - time_makeChunks;
        System.out.println("Time taken to complete the sorting: "+ totalTime);

    }
    private static void mergeArraysWithHeaps(String[] files, int totalFiles) throws IOException {
        PriorityQueue<ArrayElement> pq = new PriorityQueue<>();
        long numPolls = 0;

        List<DataInputStream> inputStreams = new ArrayList<>();
        for (int i = 0; i < totalFiles; i++) {
            try {
                inputStreams.add(new DataInputStream(new BufferedInputStream(new FileInputStream(files[i]))));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        for (int i = 0; i < totalFiles; i++) {
            try {
                DataInputStream dis = inputStreams.get(i);
                int val = dis.readInt();
                pq.offer(new ArrayElement(val, i));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("/home/priyanshu/Desktop/asgnone/outputFile")));

        while (!pq.isEmpty()) {
            ArrayElement minElement = pq.poll();
            numPolls += 1;
            int minVal = minElement.getValue();
            int chunkNumber = minElement.getChunkNumber();
            os.writeInt(minVal);
            try {
                DataInputStream dis = inputStreams.get(chunkNumber);
                int val = dis.readInt();
                pq.offer(new ArrayElement(val, chunkNumber));
            } catch (EOFException e) {
            }
        }
        System.out.println("Number of polls: " + numPolls);
    }

    private static void SortArrayAndStore(int[] toSort, int len, ExecutorService executorService, String fileName) throws IOException{

        int cores = 8;
        int lengthToSort = len / cores;

        List<Future<?>> allFuture = new ArrayList<>();
        for (int i = 0; i < cores; i++) {
            int startIdx = i * lengthToSort;
            int endIdx = startIdx + lengthToSort;
            if (i == cores - 1) {
                endIdx = (int) len;
            }
            allFuture.add(executorService.submit(new Sort(toSort, startIdx, endIdx)));
        }

        for (Future<?> future : allFuture) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        mergeSectionsOfChunk(toSort, cores, lengthToSort,len, fileName);
    }
    private static void mergeSectionsOfChunk(int[] toSort, int cores, int section_len, long len, String fileName) throws IOException{


        PriorityQueue<ArrayElement> pq = new PriorityQueue<>();
        int[] sectionIndex = new int[cores];

        for (int chunkIdx = 0; chunkIdx < cores; chunkIdx++){
            pq.offer(new ArrayElement(toSort[chunkIdx * section_len],  chunkIdx));
        }

        for (int chunkIdx = 0; chunkIdx < cores; chunkIdx++){
            sectionIndex[chunkIdx] = chunkIdx * section_len;
        }

        DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));

         for (int j = 0; j < len - cores; j++) {
             ArrayElement minElement = pq.poll();
             int minVal = minElement.getValue();
             int chunkNumber = minElement.getChunkNumber();

             os.writeInt(minVal);
             sectionIndex[chunkNumber]++;

             if (sectionIndex[chunkNumber] < (chunkNumber + 1) * section_len) {
                 pq.offer(new ArrayElement(toSort[sectionIndex[chunkNumber]], chunkNumber));
             }
         }
         os.flush();
    }
}
