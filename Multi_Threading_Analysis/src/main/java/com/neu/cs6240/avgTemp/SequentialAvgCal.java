package com.neu.cs6240.avgTemp;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.neu.cs6240.avgTemp.StationAvgCal.fileRecords;

/**
 * Created by NishantRatnakar on 9/25/2016.
 */
public class SequentialAvgCal {

	// common data structure for storing details of station
	static Map<String, StationDetails> sequentialStationList;

	//list for tracking run time of SEQ-LOCK program
	static List<Long> sequentialRuntimes = new ArrayList<>();


	public SequentialAvgCal() {

		for (int i = 0; i < 10; i++) {

			sequentialStationList = new HashMap<String, StationDetails>();

			// tracking start of program
			long startTime = System.currentTimeMillis();


			for (String newRecord : fileRecords) {
				// ensure only relevant records are processed for the calculation of average temperature
				if (newRecord.contains("TMAX")) {
					String[] recordDetails = newRecord.split(",");
					String stationID = recordDetails[0];
					double stationTemp = new Double(recordDetails[3]);

					// takes appropriate action based on the presence of the stationID in common data structure
					if (sequentialStationList.containsKey(stationID)) {

						//Since station details already present in the common data structure,
						// simply updating the details of the station
						sequentialStationList.get(stationID).updateAvgTemp(stationTemp);
					} else {

						// stationID not present hence creating accumulating data object and inserting the station details 
						// in the common data structure
						StationDetails station = new StationDetails(stationTemp, 1);
						sequentialStationList.put(stationID, station);
					}
				}
			}

			// tracking end of program
			long endTime = System.currentTimeMillis();

			// calculating execution time
			sequentialRuntimes.add(endTime - startTime);
		}


		// sorting list in ascending order and calculation total runtime
		Collections.sort(sequentialRuntimes);
		long runtimeSum = 0;
		for (long runtime : sequentialRuntimes) {
			runtimeSum += runtime;

		}


		System.out.println("Minimum runtime for Sequential: " + sequentialRuntimes.get(0));
		System.out.println("Maximum runtime for Sequential: " + sequentialRuntimes.get(9));
		System.out.println("Average runtime for Sequential: " + runtimeSum / 10);

	}

	//method implemented to display contents of data structure for verification
	private static void printStationDetails(Map<String, StationDetails> stationList) {

		for (String i : stationList.keySet()) {
			System.out.println("Station ID: " + i + stationList.get(i));
		}
	}
}
