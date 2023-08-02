package io.e6x;
import java.io.*;
import java.util.*;

import java.util.concurrent.*;

public class SortAndStore {

    public static void main(String[] args) throws IOException{
        final long lenBytes = 2L *1024* 1024*  1024;
        final long len =  lenBytes/ 4;
        int cores = 4;
        ExecutorService executorServiceToReadAndSort = Executors.newFixedThreadPool(cores);
        long time_makeChunks = System.currentTimeMillis();
        int totalFiles = 20;
        String[] files = new String[totalFiles];
        System.out.println("Reading and Sorting into chunks started");
        String sourcefile = "/home/priyanshu/Desktop/asgnone/inputFile";
        for (int i = 0; i < totalFiles; i++) {

            long fileStartIdx = i  * lenBytes;
            long timeSortEachChunk = System.currentTimeMillis();
            String fileName = "/home/priyanshu/Desktop/asgnone/inputFile" + (i + 1);
            files[i] = fileName;
            ReadAndSortAndStore((int)len, executorServiceToReadAndSort, fileName, cores, fileStartIdx, sourcefile);
            long timeElapse_sortEachChunk = System.currentTimeMillis() - timeSortEachChunk;
            System.out.println("time to read and sort and store chunk " +  i + " with parallel reading and sorting" + ": " + timeElapse_sortEachChunk);
        }

        executorServiceToReadAndSort.shutdown();
        try {
            executorServiceToReadAndSort.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long timeElapse_makeChunks = System.currentTimeMillis() - time_makeChunks;
        System.out.println("Time elapsed to make sorted chunks: " + timeElapse_makeChunks);

        System.out.println("merging of chunks started");
        long time_mergeChunks = System.currentTimeMillis();
        mergeArraysWithHeaps(files, totalFiles);
        long elapsedTime_mergeChunks = System.currentTimeMillis() - time_mergeChunks;
        System.out.println("Time passed to merge chunks: " + elapsedTime_mergeChunks);
        long totalTime = System.currentTimeMillis() - time_makeChunks;
        System.out.println("Time taken to complete the sorting: "+ totalTime);

        /*try(DataInputStream dis = new DataInputStream(new FileInputStream("/home/priyanshu/Desktop/asgnone/outputFile"))){
            for(int i=0 ;i < 100;i++){
                int val = dis.readInt();
                System.out.println(val);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }*/

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
        os.flush();
        System.out.println("Number of polls: " + numPolls);
    }
    private static void ReadAndSortAndStore(int len, ExecutorService executorService, String fileName, int cores, long fileStartIdx, String sourceFile) throws IOException{

        int lengthToSort = len / cores;
        List<Future<?>> allFuture = new ArrayList<>();
        int[] toSort = new int[len];

        for (int i = 0; i < cores; i++) {
            int arrStartIdx = i * lengthToSort;
            int arrEndIdx = (i + 1) * lengthToSort;
            allFuture.add(executorService.submit(new ReadAndSort(toSort, fileStartIdx + (lengthToSort * 4 * i) ,sourceFile, arrStartIdx, arrEndIdx)));
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

         for (int j = 0; j < len ; j++) {
             ArrayElement minElement = pq.poll();
             int minVal = minElement.getValue();
             int chunkNumber = minElement.getChunkNumber();
            //totalElem += 1;
             os.writeInt(minVal);
             sectionIndex[chunkNumber]++;

             if (sectionIndex[chunkNumber] < (chunkNumber + 1) * section_len) {
                 pq.offer(new ArrayElement(toSort[sectionIndex[chunkNumber]], chunkNumber));
             }
         }
         os.flush();
    }
}
