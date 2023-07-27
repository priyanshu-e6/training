package io.e6x;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class SortAndStore {

    public static void main(String[] args) {
        final long len = 2L * 1024 * 1024 * 1024 / 4;
        long section_len = 2L * 1024 * 1024 * 1024;
        //TODO : put for loop inside try block
        for (int i = 0; i < 25; i++) {
            int[] inputData = new int[(int) len];
            long offset = i * 2 * 1024 * 1024 *1024;
            try (DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream("/home/priyanshu/Desktop/asgnone/inputFile")))
            ) {

                is.skipBytes((int) offset);
                int j=0;
                try {
                    while (true) {
                        inputData[i] = is.readInt();
                        j += 1;
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
        mergeArraysWithHeaps((int)section_len);

    }



    private static void mergeArraysWithHeaps(int section_len) {

        long startOfMergeTime = System.currentTimeMillis();

        PriorityQueue<ArrayElement> pq = new PriorityQueue<>();

        int[] sectionIndex = new int[25];
        //TODO : dump sorted chunks into diff files
        for (int i = 0; i < 25; i++) {
            int offset = i * 2 * 1024 * 1024 *1024;
            sectionIndex[i] = offset;
        }

        try(RandomAccessFile randomAccessFile = new RandomAccessFile("/home/priyanshu/Desktop/asgnone/inputFile", "r")){
            for (int i=0;i<25;i++) {
                randomAccessFile.seek(sectionIndex[i]);
                DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(randomAccessFile.getFD())));
                int val =  dataInputStream.readInt();
                pq.offer(new ArrayElement(val, i));

            }

            try(DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("/home/priyanshu/Desktop/asgnone/outputFile")));
                DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(randomAccessFile.getFD())));){
                while(!pq.isEmpty()) {
                    ArrayElement minElement = pq.poll();
                    int minVal = minElement.getValue();
                    int chunkNumber = minElement.getChunkNumber();

                    os.writeInt(minVal);
                    sectionIndex[chunkNumber]+=32;

                    if(sectionIndex[chunkNumber] < (chunkNumber + 1)*section_len){
                        randomAccessFile.seek(sectionIndex[chunkNumber]);
                        int val = dataInputStream.readInt();
                        pq.offer(new ArrayElement(val, chunkNumber));

                    }

                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }


        }catch(Exception e){
            e.printStackTrace();
        }

    }
}






