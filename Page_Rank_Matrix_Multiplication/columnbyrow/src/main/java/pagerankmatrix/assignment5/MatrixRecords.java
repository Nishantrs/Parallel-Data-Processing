package pagerankmatrix.assignment5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

// This structure is used to store details about Matrix from which data was emitted 
// and contribution/value of page rank by the specified index.
public class MatrixRecords implements Writable{

	
	private String matrixName;
	private Long pageIndex;
	private Double pageRankContribution;
	

	// This constructor is used to store the page index and page rank value/contribution
	public MatrixRecords(Long pageIndex, Double pageRankContribution) {
		super();
		this.matrixName = new String();
		this.pageIndex = pageIndex;
		this.pageRankContribution = pageRankContribution;
	}
	
	// This constructor is used to store the matrix name, page index and page rank value/contribution
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

	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeUTF(matrixName);
		out.writeLong(pageIndex);
		out.writeDouble(pageRankContribution);

	}
}
