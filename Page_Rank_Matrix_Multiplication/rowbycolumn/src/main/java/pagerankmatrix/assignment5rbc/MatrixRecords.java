package pagerankmatrix.assignment5rbc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

// This Node structure either holds the outlinks or the parameters required by the Node to update
// the page rank
public class MatrixRecords implements Writable{

	
	private String matrixName;
	private Long pageIndex;
	private Double pageRankContribution;

	// This constructor is used to to update the page rank of the node
	

	// This constructor is used to store the outlinks of the node
	public MatrixRecords(Long pageIndex, Double pageRankContribution) {
		super();
		this.matrixName = new String();
		this.pageIndex = pageIndex;
		this.pageRankContribution = pageRankContribution;
	}
	
	public MatrixRecords(String matrixName, Long pageIndex, Double pageRankContribution) {
		super();
		this.matrixName = matrixName;
		this.pageIndex = pageIndex;
		this.pageRankContribution = pageRankContribution;
	}

	// Default constructor
	public MatrixRecords() {
		super();		
		this.matrixName = new String();
		this.pageIndex = 0l;
		this.pageRankContribution = 0.0;
	}

	// List of setters and getters
	public Long getPageIndex() {
		return pageIndex;
	}
	
	public Double getPageRankContribution() {
		return pageRankContribution;
	}
	
	public String getMatrixName() {
		return matrixName;
	}
	

	
	public void setPageIndex(Long pageIndex) {
		this.pageIndex = pageIndex;
	}
	
	public void setPageRankContribution(Double pageRankContribution) {
		this.pageRankContribution = pageRankContribution;
	}
	
	public void setMatrixName(String matrixName) {
		this.matrixName = matrixName;
	}
	

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.matrixName = in.readUTF();
		this.pageIndex = in.readLong();
		this.pageRankContribution = in.readDouble();
		//matrixName.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeUTF(matrixName);
		out.writeLong(pageIndex);
		out.writeDouble(pageRankContribution);
		//matrixName.write(out);

	}
}
