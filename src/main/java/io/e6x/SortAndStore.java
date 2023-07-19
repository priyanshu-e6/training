package io.e6x;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

public class SortAndStore {
    public static <os> void main(String[] args) {
        ArrayList<Integer> inputdata = new ArrayList<>();
        try(BufferedInputStream is = new BufferedInputStream(new FileInputStream("/home/priyanshu/Desktop/asgnone/inputFile"));
        ){

    //takes input and stores in list

            byte[] buffer = new byte[4];

            while ((is.read(buffer)) != -1) {
                int value = bytesToInt(buffer);
                inputdata.add(value);
            }
        }

        catch (IOException e) {
            e.printStackTrace();
        }
        //sort the array using inbuilt sort
        Collections.sort(inputdata);


//        for (int number : inputdata) {
//            System.out.print(number + ", ");
//        }


        //store in file output
        try(BufferedWriter os = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/priyanshu/Desktop/asgnone/outputFile")))) {
            for(int number: inputdata){
                os.write(number);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }


    }

    private static int bytesToInt(byte[] buffer) {
        return (buffer[0] & 0xFF) << 24 |
                (buffer[1] & 0xFF) << 16 |
                (buffer[2] & 0xFF) << 8 |
                (buffer[3] & 0xFF);
    }


}