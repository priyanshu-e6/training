package io.e6x;

import java.util.Arrays;

public class Sort implements Runnable{
    int[] arr;
    int startidx;
    int endidx;

    Sort(int[] arr, int startidx, int endidx){
        this.arr = arr;
        this.startidx= startidx;
        this.endidx = endidx;

    }
    public void run(){
        Arrays.sort(arr, startidx, endidx);
    }
}
