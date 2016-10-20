package com.neu.cs6240.avgTemp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by NishantRatnakar on 9/17/2016.
 */
public class StationAvgCal {

	
  static List<String> fileRecords = new ArrayList<>();



  public static void main(String[] args) throws Exception{

    csvFileLoader(args[0]);
    System.out.println("***Sequential Program with Fibonacci delay***");
    new SequentialAvgCal();
    System.out.println();
    System.out.println("----------------------------------------------");
    System.out.println();
    System.out.println("***No Lock Program with Fibonacci delay***");
    new NoLockAvgCal();
    System.out.println();
    System.out.println("----------------------------------------------");
    System.out.println();
    System.out.println("***Coarse Lock Program with Fibonacci delay***");
    new CoarseLockAvgCal();
    System.out.println();
    System.out.println("----------------------------------------------");
    System.out.println();
    System.out.println("***Fine Lock Program with Fibonacci delay***");
    new FineLockAvgCal();
    System.out.println();
    System.out.println("----------------------------------------------");
    System.out.println();
    System.out.println("***No Sharing Program with Fibonacci delay***");
    new NoSharingAvgCal();
  }

  public static void csvFileLoader(String path) throws IOException,FileNotFoundException{

    String fileName = path;
    BufferedReader br = new BufferedReader(new FileReader(fileName));
    String newLine;

    try {

      // Read data as it is till '/n'
      while ((newLine = br.readLine()) != null) {
        
        fileRecords.add(newLine);
      }
      br.close();
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("Please enter valid path for the .csv file.");
    }



  }
}

