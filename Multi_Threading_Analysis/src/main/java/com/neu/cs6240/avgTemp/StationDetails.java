package com.neu.cs6240.avgTemp;

/**
 * Created by NishantRatnakar on 9/17/2016.
 */
public class StationDetails {

	double tempSum;
	int counter;

	@Override
	public String toString() {
		return ", avgTemp= " + getAvgTemp();
	}

	public StationDetails(double tempSum, int counter) {
		this.tempSum = tempSum;
		this.counter = counter;
	}

	public double getAvgTemp() {
		return tempSum/counter;
	}

	public double getTempSum(){
		return tempSum;
	}

	public int getCounter() {
		return counter;
	}


	// method to update station details of an already existing station in common data structure
	public void updateAvgTemp(double temp){
		Fibonacci(17);
		counter++;
		this.tempSum = temp + this.tempSum;
	}

	// overloaded method to update station details of an already existing station in common data structure 
	public void updateAvgTemp(double otherTempSum, int otherCount){
		Fibonacci(17);
		this.counter = otherCount + this.counter;
		this.tempSum = otherTempSum + this.tempSum;
	}

	// method to access accumulating data object
	public synchronized void updateAvgTempSync(double temp){
		Fibonacci(17);
		counter++;
		this.tempSum = temp + this.tempSum;
	}

	// Delay method
	public void Fibonacci(int value){
		int x = 0, y = 1, z = 1;

		for (int i = 0; i < value; i++) {
			x = y;
			y = z;
			z = x + y;
		}
	}
}
