package mapreducesecavgcal.mrssavgcal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StationInstanceInCombine implements Writable{

	private Double acctmax;
	private Double acctmin;
	private Integer countTmax;
	private Integer countTmin;


	public StationInstanceInCombine() {
		// need this for serialization
	}

	public Double getAccTmax() {
		return acctmax;
	}

	public Double getAccTmin() {
		return acctmin;
	}

	public StationInstanceInCombine(Double acctmax, Double acctmin, Integer countTmax, Integer countTmin) {
		super();
		this.acctmax = acctmax;
		this.acctmin = acctmin;
		this.countTmax = countTmax;
		this.countTmin = countTmin;
	}

	public Integer getCountTmax() {
		return countTmax;
	}

	public Integer getCountTmin() {
		return countTmin;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.acctmax = in.readDouble();
		this.acctmin = in.readDouble();
		this.countTmax = in.readInt();
		this.countTmin = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(acctmax);
		out.writeDouble(acctmin);
		out.writeInt(countTmax);
		out.writeInt(countTmin);
	}	

	@Override
	public int hashCode() {
		return (int)new Double(acctmax).hashCode()
				+ (int)new Double(acctmin).hashCode()
				+ new Integer(countTmax).hashCode()
				+ new Integer(countTmin).hashCode();
	}

	// Updating the accumulated minimum average temperature and count of Station
	public void updateTmin(Double temp){
		this.acctmin = this.acctmin + temp;
		countTmin++;
	}

	// Updating the accumulated maximum average temperature and count of Station
	public void updateTmax(Double temp){
		this.acctmax = this.acctmax + temp;
		countTmax++;
	}

	// Updating the accumulated average temperature and count of Station adding the values from other instance of same station
	public void updateStationInstance(Double otheracctmax,Double otheracctmin,Integer othercountTmax, Integer othercountTmin){
		this.acctmax += otheracctmax;
		this.acctmin += otheracctmin;
		this.countTmax += othercountTmax;
		this.countTmin += othercountTmin;
	}

}

