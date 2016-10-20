package mapreduceavgcal.mravgcal;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StationInstance implements Writable{
	
	private Double tmax;
	private Double tmin;
	private Boolean isTmax;
	
	
	public StationInstance() {
        // need this for serialization
    }
	
	public Double getTmax() {
		return tmax;
	}

	public Boolean isTmax() {
		return isTmax;
	}

	public Double getTmin() {
		return tmin;
	}

	public StationInstance(double tmax, double tmin, boolean isTmax) {
		super();
		this.tmax = tmax;
		this.tmin = tmin;
		this.isTmax = isTmax;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.tmax = in.readDouble();
		this.tmin = in.readDouble();
		this.isTmax = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(tmax);
		out.writeDouble(tmin);
		out.writeBoolean(isTmax);
	}
	
	@Override
	public int hashCode() {
        return (int)new Double(tmax).hashCode() + (int)new Double(tmin).hashCode();
    }

}
