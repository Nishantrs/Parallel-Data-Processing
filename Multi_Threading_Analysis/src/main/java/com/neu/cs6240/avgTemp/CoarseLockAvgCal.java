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

class CoarseLockThread extends Thread {

	private int min;
	private int max;

	public CoarseLockThread(int min, int max){
		this.min = min;
		this.max = max;
	}
	@Override
	public void run() {

		// ensures that thread iterates over each record present in the .csv file
		for (int i = min; i < max ; i++) {

			CoarseLockAvgCal.stationListUpdate(i);

		}

	}
}


public class CoarseLockAvgCal {


	// common data structure for storing details of station
	static Map<String, StationDetails> coarseLockStationList;

	// list for tracking run time of COARSE-LOCK program
	static List<Long> coarseLockRuntimes = new ArrayList<>();

	public CoarseLockAvgCal() throws Exception {

		for (int i = 0; i < 10; i++) {


			coarseLockStationList = new HashMap<>();

			// tracking start of program
			long startTime = System.currentTimeMillis();

			
			// Distributing records in .csv files among different threads
			CoarseLockThread t1 = new CoarseLockThread(0, fileRecords.size() / 2);
			CoarseLockThread t2 = new CoarseLockThread(fileRecords.size() / 2, fileRecords.size());

			//Experimenting with higher number of threads
			//    CoarseLockThread t1 = new CoarseLockThread(0, relevantFileRecords.size() / 3);
			//    CoarseLockThread t2 = new CoarseLockThread(relevantFileRecords.size() / 2, 2*relevantFileRecords.size()/3);
			//    CoarseLockThread t3 = new CoarseLockThread(2*relevantFileRecords.size()/3, relevantFileRecords.size());


			t1.start();
			t2.start();
			//    t3.start();

			t1.join();
			t2.join();
			//    t3.join();


			// tracking end of program
			long endTime = System.currentTimeMillis();

			// calculating execution time
			coarseLockRuntimes.add(endTime - startTime);

		}

		// sorting list in ascending order and calculation total runtime
		Collections.sort(coarseLockRuntimes);
		long runtimeSum = 0;
		for (long runtime : coarseLockRuntimes) {
			runtimeSum += runtime;

		}


		System.out.println("Minimum runtime for Coarse Lock: " + coarseLockRuntimes.get(0));
		System.out.println("Maximum runtime for Coarse Lock: " + coarseLockRuntimes.get(9));
		System.out.println("Average runtime for Coarse Lock: " + runtimeSum / 10);

	}


	// method implemented to display contents of data structure for verification
	private static void printStationDetails(Map<String, StationDetails> stationList) {

		for (String i : stationList.keySet()){
			System.out.println("Station ID: " + i + stationList.get(i));
		}
	}

	// only one thread can access this method at a time. This method provides
	// access to the common data structure.
	public static synchronized void stationListUpdate(int index) {

		String newRecord = fileRecords.get(index);

		// ensure only relevant records are processed for the calculation of average temperature
		if (newRecord.contains("TMAX")) {
			String[] recordDetails = newRecord.split(",");
			String stationID = recordDetails[0];
			double stationTemp = new Double(recordDetails[3]);

			// takes appropriate action based on the presence of the stationID in common data structure
			if (!coarseLockStationList.containsKey(stationID)) {

				// stationID not present hence creating accumulating data object and inserting the station details 
				// in the common data structure
				StationDetails station = new StationDetails(stationTemp, 1);
				coarseLockStationList.put(stationID, station);

			} else {
				//Since station details already present in the common data structure,
				// simply updating the details of the station
				coarseLockStationList.get(stationID).updateAvgTemp(stationTemp);
			}
		}

	}
}
