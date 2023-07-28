package io.e6x;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class checkIfSorted{
    long section_len = 50L * 1024 * 1024 * 1024;
    public static void main(String[] args){
        try(DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream("/home/priyanshu/Desktop/asgnone/outputFile")))){
            int compare = dataInputStream.readInt();
            int flag=0;
            for(int i=0;i<;i++){
                int number = dataInputStream.readInt();
                if(number < compare){
                    flag = 1;
                    break;
                }
            }
            if(flag){
                System.out.println("data is not sorted");
            }
            else{
                System.out.println("Data is sorted");
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}