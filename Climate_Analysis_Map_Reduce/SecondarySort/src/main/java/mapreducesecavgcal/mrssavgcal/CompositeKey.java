package mapreducesecavgcal.mrssavgcal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

// This class is used as key for Secondary Sorting
public class CompositeKey implements WritableComparable{

	private String stationID;
	private Integer year;

	public CompositeKey() {

	}

	public CompositeKey(String stationID, Integer year) {
		super();
		this.stationID = stationID;
		this.year = year;
	}

	public String getStationID() {
		return stationID;
	}

	public Integer getYear() {
		return year;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.year = in.readInt();
		this.stationID = in.readUTF();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(year);
		out.writeUTF(stationID);

	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((stationID == null) ? 0 : stationID.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CompositeKey other = (CompositeKey) obj;
		if (stationID == null) {
			if (other.stationID != null)
				return false;
		} else if (!stationID.equals(other.stationID))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		CompositeKey other = (CompositeKey)o;

		if(this.stationID.equals(other.stationID)){
			
			return this.year.compareTo(other.year);
			
		}else{
			
			return this.stationID.compareTo(other.stationID);
		}
	}
}
