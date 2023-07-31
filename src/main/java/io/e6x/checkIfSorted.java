package io.e6x;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class checkIfSorted{

    public static void main(String[] args){
        String filePath = "/home/priyanshu/Desktop/asgnone/outputFile";
        if (isFileSorted(filePath)) {
            System.out.println("The data in the file is sorted.");
        } else {
            System.out.println("The data in the file is not sorted.");
        }
    }

    public static boolean isFileSorted(String filePath) {
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filePath)))) {
            int prevValue = Integer.MIN_VALUE;

            while (dis.available() > 0) {
                int currentValue = dis.readInt();
                if (currentValue < prevValue) {
                    return false;
                }
                prevValue = currentValue;
            }

            return true; // Data is sorted
        } catch (IOException e) {
            e.printStackTrace();
            return false; // Error occurred, consider data as not sorted
        }
    }
}