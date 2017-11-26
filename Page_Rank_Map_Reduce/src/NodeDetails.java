package pagerank.assignment3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

// This Node structure either holds the outlinks or the parameters required by the Node to update
// the page rank
public class NodeDetails implements Writable{

	private Long otherInlink;
	private Double otherPageRankValue;
	private Text outlinks;
	private Boolean hasOutlinks;

	// This constructor is used to to update the page rank of the node
	public NodeDetails(String outlinks, Boolean hasOutlinks) {
		super();
		this.otherInlink = 0l;
		this.otherPageRankValue = 0.0;
		this.outlinks = new Text(outlinks);
		this.hasOutlinks = hasOutlinks;
	}

	// This constructor is used to store the outlinks of the node
	public NodeDetails(Long otherInlink, Double otherPageRankValue) {
		super();
		this.otherInlink = otherInlink;
		this.otherPageRankValue = otherPageRankValue;
		this.outlinks = new Text();
		this.hasOutlinks = false;
	}

	// Default constructor
	public NodeDetails() {
		super();
		this.otherInlink = 0l;
		this.otherPageRankValue = 0.0;
		this.outlinks = new Text();
		this.hasOutlinks = false;
	}

	// List of setters and getters
	public Long getOtherInlink() {
		return otherInlink;
	}
	
	public Double getOtherPageRankValue() {
		return otherPageRankValue;
	}
	
	public String getOutlinks() {
		return outlinks.toString();
	}
	
	public Boolean isHasOutlinks() {
		return hasOutlinks;
	}
	
	public void setOtherInlink(Long otherInlink) {
		this.otherInlink = otherInlink;
	}
	
	public void setOtherPageRankValue(Double otherPageRankValue) {
		this.otherPageRankValue = otherPageRankValue;
	}
	
	public void setOutlinks(String outlinks) {
		this.outlinks = new Text(outlinks);
	}
	
	public void setHasOutlinks(boolean hasOutlinks) {
		this.hasOutlinks = hasOutlinks;
	}
	@Override
	public void readFields(DataInput in) throws IOException {

		this.otherInlink = in.readLong();
		this.otherPageRankValue = in.readDouble();
		//this.outlinks = in.readUTF(outlinks);
		outlinks.readFields(in);
		this.hasOutlinks = in.readBoolean();

	}
	@Override
	public void write(DataOutput out) throws IOException {

		out.writeLong(otherInlink);
		out.writeDouble(otherPageRankValue);
		//out.writeUTF(outlinks);
		outlinks.write(out);
		out.writeBoolean(hasOutlinks);

	}
}
