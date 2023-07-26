package io.e6x;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.*;

public class SortAndStore {

    public static void main(String[] args) {
        final long len = 2L * 1024 * 1024 * 1024 / 4;
        int[] inputData = new int[(int) len];


        int numsRead = 0;
        try (DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream("/home/priyanshu/Desktop/asgnone/inputFile")))
        ) {

            //takes input and stores in list

            int i=0;
            try {
                while (true) {
                    inputData[i] = is.readInt();
                    i += 1;
                }
            } catch (EOFException e) {
                // End-of-file reached, stop reading
            }

            System.out.println("Integers read: " + i);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //using executor service class to solve the sorting using concurrency

        int cores = Runtime.getRuntime().availableProcessors();
        int lengthToSort = (int) (len / cores);

        long startTime = System.currentTimeMillis();

        ExecutorService executorService = Executors.newFixedThreadPool(cores);
        List<Future<?>> allFuture = new ArrayList<>();

        for (int i = 0; i < cores; i++) {
            int startIdx = i * lengthToSort;
            int endIdx = startIdx + lengthToSort;
            if (i == cores - 1) {

                endIdx = (int) len;
            }
            allFuture.add(executorService.submit(new Sort(inputData, startIdx, endIdx)));
        }

        for (Future<?> future : allFuture) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        System.out.println("Execution Time for sorting parallel: " + executionTime + " milliseconds");
        System.out.println("Memory used: " + (Runtime.getRuntime().totalMemory() / (1024 * 1024)) + " MB");

        //merge the sorted section
        long startTime2 = System.currentTimeMillis();

        int sectionSize = (int) len / cores;

         //using merging of parts by iterating

        /*for (int i = 1; i < cores; i++) {
            mergeSortedSections(inputData, 0, i * sectionSize - 1, (i+1) * sectionSize  - 1);
        }*/


        //merging using min heap


        mergeArraysWithHeaps(inputData, cores, sectionSize, len);


        System.out.println("Memory used: " + (Runtime.getRuntime().totalMemory() / (1024 * 1024)) + " MB");

        long endTime2 = System.currentTimeMillis();
        long executionTime2 = endTime2 - startTime2;
        System.out.println("Execution Time for merging: " + executionTime2 + " milliseconds");

        //final check
        int flag = 0;
        for (int i = 1; i < len; i++) {
            if (inputData[i] < inputData[i - 1]) {
                flag = 1;
                break;
            }
        }

        System.out.println("Memory used: " + (Runtime.getRuntime().totalMemory() / (1024 * 1024)) + " MB");

        if (flag == 0) {
            System.out.println("data is sorted");

            //store in file output
            try (BufferedWriter os = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/priyanshu/Desktop/asgnone/outputFile")))) {
                for (int number : inputData) {
                    os.write(number);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("Output file created for sorted data.");
        } else {
            System.out.println("data is not sorted");
        }
    }


    private static void mergeArraysWithHeaps(int[] inputData, int cores, int section_len, long len) {

        long startOfMergeTime = System.currentTimeMillis();

        PriorityQueue<ArrayElement> pq = new PriorityQueue<>();
        int[] sortedData = new int[(int) len];
        int[] sectionIndex = new int[cores];

        for (int chunkIdx = 0; chunkIdx < cores; chunkIdx++){
            pq.offer(new ArrayElement(inputData[chunkIdx * section_len],  chunkIdx));

        }
        for (int chunkIdx = 0; chunkIdx < cores; chunkIdx++){
            sectionIndex[chunkIdx] = chunkIdx * section_len;
        }

        int i = 0;
        for (int j = 0; j < len - cores; j++){
            ArrayElement minElement = pq.poll();
            int minVal = minElement.getValue();
            int chunkNumber = minElement.getChunkNumber();

            inputData[i] = minVal;
            sectionIndex[chunkNumber]++;

            if (sectionIndex[chunkNumber] < (chunkNumber + 1) * section_len) {
                pq.offer(new ArrayElement(inputData[sectionIndex[chunkNumber]],  chunkNumber));
            }
            i++;

        }
        long elapsedTime = (System.currentTimeMillis() - startOfMergeTime);
        System.out.println("Time for merge function: " + elapsedTime + " ms");
        System.arraycopy(sortedData, 0, inputData, 0, (int) len);
    }
}






