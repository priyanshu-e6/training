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
        try  {
            RandomAccessFile raf = new RandomAccessFile(filename, "r");
            raf.seek(fileStartIdx);
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(raf.getFD()));

            // Wrap BufferedInputStream in InputStreamReader and BufferedReader for reading lines
            InputStreamReader isr = new InputStreamReader(bis);
            BufferedReader br = new BufferedReader(isr);
            int idx = arrIdxStart;
            byte[] buffer = new byte[4];
            while (idx < arrIdxEnd) {

                int bytesRead = bis.read(buffer);
                if (bytesRead != -1) {
                    // Process the read data (e.g., convert bytes to an integer)
                    int intValue = ((buffer[3] & 0xFF) << 24) |
                            ((buffer[2] & 0xFF) << 16) |
                            ((buffer[1] & 0xFF) << 8) |
                            (buffer[0] & 0xFF);
                    arr[idx] = intValue;
                    idx+=1;
                }
            }
            br.close();
            bis.close();
            raf.close();

            Arrays.sort(arr, arrIdxStart, idx);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
