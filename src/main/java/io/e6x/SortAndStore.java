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

        // TODO: put buffered input stream inside file input stream to get function of getting int directly with getInt method
        int numsRead = 0;
        try (BufferedInputStream is = new BufferedInputStream(new FileInputStream("/home/priyanshu/Desktop/asgnone/inputFile"))
        ) {

            //takes input and stores in list
            byte[] buffer = new byte[4];

            while ((is.read(buffer)) != -1) {
                int value = bytesToInt(buffer);
                inputData[numsRead] = (value);
                numsRead += 1;
            }

            System.out.println("Integers read: " + numsRead);
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


    private static int bytesToInt(byte[] buffer) {
        return (buffer[0] & 0xFF) << 24 |
                (buffer[1] & 0xFF) << 16 |
                (buffer[2] & 0xFF) << 8 |
                (buffer[3] & 0xFF);
    }

    private static void mergeSortedSections(int[] inputData, int start1, int start2, int end) {
        long startOfMergeTime = System.currentTimeMillis();
        int[] temp = new int[end - start1 + 1];
        int i = start1, j = start2 + 1, k = 0;

        while (i <= start2 && j <= end) {
            if (inputData[i] <= inputData[j]) {
                temp[k] = inputData[i];
                i++;
            } else {
                temp[k] = inputData[j];
                j++;
            }
            k++;
        }

        while (i <= start2) {
            temp[k] = inputData[i];
            i++;
            k++;
        }

        while (j <= end) {
            temp[k] = inputData[j];
            j++;
            k++;
        }

        System.arraycopy(temp, 0, inputData, 0, end - start1 + 1);
        long elapsedTime = (System.currentTimeMillis() - startOfMergeTime);
        System.out.println("Time for merge function: " + elapsedTime + " ms");
    }
}







