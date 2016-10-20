package com.neu.cs6240.avgTemp;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.neu.cs6240.avgTemp.StationAvgCal.fileRecords;

/**
 * Created by NishantRatnakar on 9/20/2016.
 */


class NoLockThread extends Thread {

	private int min;
	private int max;

	public NoLockThread(int min, int max) {
		this.min = min;
		this.max = max;
	}

	@Override
	public void run() {

		for (int i = min; i < max; i++) {

			// ensures that thread iterates over each record present in the .csv file
			NoLockAvgCal.stationListUpdate(i);

		}

	}
}

public class NoLockAvgCal {

	// common data structure for storing details of station
	static Map<String, StationDetails> noLockStationList;

	//list for tracking run time of NO-LOCK program
	static List<Long> noLockRuntimes = new ArrayList<>();


	public NoLockAvgCal() throws Exception {

		for (int i = 0; i < 10; i++) {

			noLockStationList = new HashMap<String, StationDetails>();

			// tracking start of program
			long startTime = System.currentTimeMillis();

			// Distributing records in .csv files among different threads
			NoLockThread t1 = new NoLockThread(0, fileRecords.size() / 2);
			NoLockThread t2 = new NoLockThread(fileRecords.size() / 2, fileRecords.size());

			// Experimenting with higher number of threads
			//    NoLockThread t1 = new NoLockThread(0, fileRecords.size() / 3);
			//    NoLockThread t2 = new NoLockThread(fileRecords.size() / 2, 2*fileRecords.size()/3);
			//    NoLockThread t3 = new NoLockThread(2*fileRecords.size()/3, fileRecords.size());

			t1.start();
			t2.start();
			//t3.start();

			t1.join();
			t2.join();
			//t3.join();


			// tracking end of program
			long endTime = System.currentTimeMillis();

			// calculating execution time
			noLockRuntimes.add(endTime - startTime);
		}

		// sorting list in ascending order and calculation total runtime
		Collections.sort(noLockRuntimes);

		long runtimeSum = 0;

		for (long runtime : noLockRuntimes) {
			runtimeSum += runtime;

		}

		System.out.println("Minimum runtime for No Lock: " + noLockRuntimes.get(0));
		System.out.println("Maximum runtime for No Lock: " + noLockRuntimes.get(9));
		System.out.println("Average runtime for No Lock: " + runtimeSum / 10);

	}

	//method implemented to display contents of data structure for verification
	private static void printStationDetails(Map<String, StationDetails> stationList) {

		for (String i : stationList.keySet()) {
			System.out.println("Station ID: " + i + stationList.get(i));
		}
	}

	//This method provides access to the common data structure. No restriction on
	// number of threads accessing the method
	public static void stationListUpdate(int index) {

		String newRecord = fileRecords.get(index);

		// ensure only relevant records are processed for the calculation of average temperature
		if (newRecord.contains("TMAX")) {
			String[] recordDetails = newRecord.split(",");
			String stationID = recordDetails[0];
			double stationTemp = new Double(recordDetails[3]);

			if (noLockStationList.containsKey(stationID)) {

				//Since station details already present in the common data structure,
				// simply updating the details of the station
				noLockStationList.get(stationID).updateAvgTemp(stationTemp);

			} else {

				// stationID not present hence creating accumulating data object and inserting the station details 
				// in the common data structure
				StationDetails station = new StationDetails(stationTemp, 1);
				noLockStationList.put(stationID, station);
			}
		}

	}
}
