package io.e6x;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class SortAndStore {

    public static void main(String[] args) {
        final long len = 2L * 1024 * 1024 * 1024 / 4;


        for (int i = 0; i < 25; i++) {
            int[] inputData = new int[(int) len];
            long offset = i * 2 * 1024 * 1024 *1024;
            try (DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream("/home/priyanshu/Desktop/asgnone/inputFile")))
            ) {

                is.skipBytes((int) offset);
                int i=0;
                try {
                    while (true) {
                        inputData[i] = is.readInt();
                        i += 1;
                    }
                } catch (EOFException e) {

                }

                System.out.println("Integers read: " + i);
            } catch (IOException e) {
                e.printStackTrace();
            }
            Arrays.sort(inputData);

            try (   FileOutputStream fileOutputStream = new FileOutputStream("/home/priyanshu/Desktop/asgnone/inputFile");
                    DataOutputStream os = new DataOutputStream(new BufferedOutputStream(fileOutputStream))) {
                fileOutputStream.getChannel().position((int) offset);
                for (int number : inputData) {
                    os.writeInt(number);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        mergeArraysWithHeaps();

        //merge the files stored in hard disk



    }



    private static void mergeArraysWithHeaps(int[] inputData, int cores, int section_len, long len) {

        long startOfMergeTime = System.currentTimeMillis();

        PriorityQueue<ArrayElement> pq = new PriorityQueue<>();
        int[] sortedData = new int[(int) len];
        int[] sectionIndex = new int[25];

        for (int i = 0; i < 25; i++) {
            int offset = i * 2 * 1024 * 1024 *1024;
            sectionIndex[i] = offset;
        }

        try(RandomAccessFile randomAccessFile = new RandomAccessFile("/home/priyanshu/Desktop/asgnone/inputFile", "r")){

        }catch(Exception e){
            e.printStackTrace();
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






