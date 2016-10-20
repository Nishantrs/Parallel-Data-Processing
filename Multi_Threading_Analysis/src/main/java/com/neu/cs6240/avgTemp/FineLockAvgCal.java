package com.neu.cs6240.avgTemp;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static com.neu.cs6240.avgTemp.StationAvgCal.fileRecords;

/**
 * Created by NishantRatnakar on 9/21/2016.
 */

class FineLockThread extends Thread {

	private int min;
	private int max;

	public FineLockThread(int min, int max){
		this.min = min;
		this.max = max;
	}
	@Override
	public void run() {

		// ensures that thread iterates over each record present in the .csv file
		for (int i = min; i < max ; i++) {

			FineLockAvgCal.stationListUpdate(i);

		}

	}
}

public class FineLockAvgCal {

	// common data structure for storing details of station
	static Map<String, StationDetails> fineLockStationList;

	// list for tracking run time of FINE-LOCK program
	static List<Long> fineLockRuntimes = new ArrayList<>();


	public FineLockAvgCal() throws Exception {

		for (int i = 0; i < 10; i++) {

			fineLockStationList = new HashMap<>();


			// tracking start of program
			long startTime = System.currentTimeMillis();


			// Distributing records in .csv files among different threads
			FineLockThread t1 = new FineLockThread(0, fileRecords.size() / 2);
			FineLockThread t2 = new FineLockThread(fileRecords.size() / 2, fileRecords.size());

			//Experimenting with higher number of threads
			//    FineLockThread t1 = new FineLockThread(0, relevantFileRecords.size() / 3);
			//    FineLockThread t2 = new FineLockThread(relevantFileRecords.size() / 2, 2*relevantFileRecords.size()/3);
			//    FineLockThread t3 = new FineLockThread(2*relevantFileRecords.size()/3, relevantFileRecords.size());


			t1.start();
			t2.start();
			//t3.start();

			t1.join();
			t2.join();
			//t3.join();


			// tracking end of program
			long endTime = System.currentTimeMillis();

			// calculating execution time
			fineLockRuntimes.add(endTime - startTime);
		}


		// sorting list in ascending order and calculation total runtime
		Collections.sort(fineLockRuntimes);

		long runtimeSum = 0;
		for (long runtime : fineLockRuntimes) {
			runtimeSum += runtime;

		}

		System.out.println("Minimum runtime of program: " + fineLockRuntimes.get(0));
		System.out.println("Maximum runtime of program: " + fineLockRuntimes.get(9));
		System.out.println("Average runtime of Program: " + runtimeSum/10);
	}

	// method implemented to display contents of data structure for verification
	private static void printStationDetails(Map<String, StationDetails> stationList) {

		for (String i : stationList.keySet()){
			System.out.println("Station ID: " + i + stationList.get(i));
		}
	}

	// This method provides access to the common data structure.
	public static void stationListUpdate(int index) {

		String newRecord = fileRecords.get(index);

		// ensure only relevant records are processed for the calculation of average temperature
		if (newRecord.contains("TMAX")) {
			String[] recordDetails = newRecord.split(",");
			String stationID = recordDetails[0];
			double stationTemp = new Double(recordDetails[3]);

			//Since station details already present in the common data structure,
			// simply updating the details of the station but the accumulating data
			// object can be accessed by only one thread at a time. If the accumulating
			// data object is free, the thread would simply proceed with update in 
			// common data structure
			if (fineLockStationList.containsKey(stationID)) {

				fineLockStationList.get(stationID).updateAvgTempSync(stationTemp);

			} else {

				// stationID not present hence inserting the station details in the common data
				// structure
				StationDetails station = new StationDetails(stationTemp, 1);

				// But during creation of accumulating data object if another thread with the same 
				// stationID does appear, there would be data corruption as first thread would insert 
				// the station record and the second thread would simply override the details. In order to make
				// sure this does not happen, a synchronized lock is created on the accumulating data object
				synchronized(station){
					if(fineLockStationList.containsKey(stationID)){

						fineLockStationList.get(stationID).updateAvgTempSync(stationTemp);
					}
					else{

						fineLockStationList.put(stationID, station);        		
					}
				}
			}
		}
	}
}
