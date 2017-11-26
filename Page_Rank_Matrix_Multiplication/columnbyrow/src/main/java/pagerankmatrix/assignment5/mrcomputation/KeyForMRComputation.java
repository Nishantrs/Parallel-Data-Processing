package pagerankmatrix.assignment5.mrcomputation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

// Custom Key for calculation of page contribution to its outlinks
public class KeyForMRComputation  implements WritableComparable<KeyForMRComputation>, Writable{
	
	private Long pageIndex;
	private String matrixName;
	
	// Constructor storing the index of the page and the matrix from which the page record was emitted
	public KeyForMRComputation(Long pageIndex, String matrixName) {
		super();
		this.pageIndex = pageIndex;
		this.matrixName = matrixName;
	}
	
	// Default constructor
	public KeyForMRComputation() {
		super();
		this.pageIndex = 0l;
		this.matrixName = new String();
	}



	// List of setters and getters
	public Long getPageIndex() {
		return pageIndex;
	}

	public String getMatrixName() {
		return matrixName;
	}

	public void setPageIndex(Long pageIndex) {
		this.pageIndex = pageIndex;
	}

	public void setMatrixName(String matrixName) {
		this.matrixName = matrixName;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		this.pageIndex = in.readLong();
		this.matrixName = in.readUTF();
		
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		out.writeLong(pageIndex);
		out.writeUTF(matrixName);
		
	}

	@Override
	public int compareTo(KeyForMRComputation o) {
		// TODO Auto-generated method stub
		
		// Arrange in descending order
		int cmp = o.getPageIndex().compareTo(this.pageIndex);
		
		// If index is same then record from R-Matrix should appear before those of M-Matrix
		if(cmp != 0){
			
			return cmp;
			
		}
							
			return o.getMatrixName().compareTo(this.matrixName);
				
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((matrixName == null) ? 0 : matrixName.hashCode());
		result = prime * result
				+ ((pageIndex == null) ? 0 : pageIndex.hashCode());
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
		KeyForMRComputation other = (KeyForMRComputation) obj;
		if (matrixName == null) {
			if (other.matrixName != null)
				return false;
		} else if (!matrixName.equals(other.matrixName))
			return false;
		if (pageIndex == null) {
			if (other.pageIndex != null)
				return false;
		} else if (!pageIndex.equals(other.pageIndex))
			return false;
		return true;
	}
}
