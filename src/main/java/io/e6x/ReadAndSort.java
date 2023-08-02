package io.e6x;

import java.io.*;
import java.util.Arrays;

public class ReadAndSort  implements Runnable {
    int[] arr;
    long fileStartIdx;//of file
    String filename;
    int arrIdxStart ;
    int arrIdxEnd;

    ReadAndSort(int[] arr, long startidx, String filename, int arrIdxStart, int arrIdxEnd){
        this.arr = arr;
        this.fileStartIdx = startidx;

        this.filename = filename;
        this.arrIdxStart = arrIdxStart;
        this.arrIdxEnd = arrIdxEnd;
    }
    public void run(){
        try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {
            raf.seek(fileStartIdx);
            int idx = arrIdxStart;

            while (idx < arrIdxEnd) {
                // Process or store the read integer as needed
                int val = 0;
                try {
                    val = raf.readInt();
                    arr[idx] = val;
                    System.out.println("value read: " + val);
                    idx += 1;
                } catch (EOFException e) {

                    break;
                }
            }
            // Sort only the elements that were read in this chunk
            Arrays.sort(arr, arrIdxStart, idx);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
