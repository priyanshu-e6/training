package io.e6x;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class checkIfSorted{

    public static void main(String[] args){
        long len = 50L * 1024 * 1024 * 1024;
        try(DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream("/home/priyanshu/Desktop/asgnone/outputFile")))){
            int compare = dataInputStream.readInt();
            int flag=0;
            for(long i=0;i<len-32;i++){
                int number = dataInputStream.readInt();
                if(number < compare){
                    flag = 1;
                    break;
                }
            }
            if(flag == 1){
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