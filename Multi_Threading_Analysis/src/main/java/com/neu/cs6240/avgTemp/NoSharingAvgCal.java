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

class NoSharingThread extends Thread {

	// local data structure for each thread
	private Map<String, StationDetails> localStationList = new HashMap<>();

	private int min;
	private int max;

	public NoSharingThread(int min, int max) {
		this.min = min;
		this.max = max;
	}

	@Override
	public void run() {

		// ensures that thread iterates over each record present in the .csv file
		for (int i = min; i < max; i++) {

			String newRecord = fileRecords.get(i);

			// ensure only relevant records are processed for the calculation of average temperature
			if (newRecord.contains("TMAX")) {
				String[] recordDetails = newRecord.split(",");
				String stationID = recordDetails[0];
				double stationTemp = new Double(recordDetails[3]);

				if (localStationList.containsKey(stationID)) {

					//Since station details already present in the local data structure,
					// simply updating the details of the station
					localStationList.get(stationID).updateAvgTemp(stationTemp);

				} else {

					// stationID not present hence creating accumulating data object and inserting the station details 
					// in the local data structure
					StationDetails station = new StationDetails(stationTemp, 1);
					localStationList.put(stationID, station);
				}
			}
		}
	}

	// method to access the local data structure for later accumulation
	public Map<String, StationDetails> getLocalStationList() {
		return localStationList;
	}
}

public class NoSharingAvgCal {

	// accumulated common data structure for storing details of station
	static Map<String, StationDetails> accumulatedStationList;

	//list for tracking run time of NO-SHARING program
	List<Long> noSharingRuntimes = new ArrayList<>();

	public NoSharingAvgCal() throws Exception {


		for (int i = 0; i < 10; i++) {


			accumulatedStationList = new HashMap<>();

			// tracking start of program
			long startTime = System.currentTimeMillis();


			// Distributing records in .csv files among different threads
			NoSharingThread t1 = new NoSharingThread(0, fileRecords.size() / 2);
			NoSharingThread t2 = new NoSharingThread(fileRecords.size() / 2, fileRecords.size());

			// Experimenting with higher number of threads
			//    NoSharingThread t1 = new NoSharingThread(0, relevantFileRecords.size() / 3);
			//    NoSharingThread t2 = new NoSharingThread(relevantFileRecords.size() / 2, 2*relevantFileRecords.size()/3);
			//    NoSharingThread t3 = new NoSharingThread(2*relevantFileRecords.size()/3, relevantFileRecords.size());


			t1.start();
			t2.start();
			//t3.start();

			t1.join();
			t2.join();
			//t3.join();

			// accumulating data from local data structure in the threads
			accumulatedStationList.putAll(t1.getLocalStationList());
			for (String s : t2.getLocalStationList().keySet()) {

				// ensuring that no station detail is overridden
				if (accumulatedStationList.containsKey(s)) {

					int otherCounter = t2.getLocalStationList().get(s).getCounter();
					double otherTempSum = t2.getLocalStationList().get(s).getTempSum();

					accumulatedStationList.get(s).updateAvgTemp(otherTempSum, otherCounter);
				} else {
					accumulatedStationList.put(s, t2.getLocalStationList().get(s));
				}
			}


			// tracking end of program
			long endTime = System.currentTimeMillis();

			// calculating execution time
			noSharingRuntimes.add(endTime - startTime);

		}

		// sorting list in ascending order and calculation total runtime
		Collections.sort(noSharingRuntimes);
		long runtimeSum = 0;
		for (long runtime : noSharingRuntimes) {
			runtimeSum += runtime;

		}

		System.out.println("Minimum runtime of program: " + noSharingRuntimes.get(0));
		System.out.println("Maximum runtime of program: " + noSharingRuntimes.get(9));
		System.out.println("Average runtime of Program: " + runtimeSum / 10);

	}

	//method implemented to display contents of data structure for verification
	private static void printStationDetails(Map<String, StationDetails> stationList) {

		for (String i : stationList.keySet()) {
			System.out.println("Station ID: " + i + stationList.get(i));
		}
	}

}
