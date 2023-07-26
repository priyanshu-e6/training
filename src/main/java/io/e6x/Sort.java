package io.e6x;

import java.util.Arrays;

public class Sort implements Runnable {
    int[] arr;
    int startIdx;
    int endIdx;


    public Sort(int[] arr, int startIdx, int endIdx){
        this.arr = arr;
        this.startIdx = startIdx;
        this.endIdx = endIdx;
    }

    public void run()  {
        Arrays.sort(arr, startIdx, endIdx);
    }
}
