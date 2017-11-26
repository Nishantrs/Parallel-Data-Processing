package pagerankmatrix.assignment5.mrcomputation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

// Custom value for calculation of page contribution to its outlinks
public class ValueForMRComputation implements Writable{
	
	private Long outlinksPageIndex;
	private Double pageRankContributionValue;

	// Constructor storing the index of outlinks and the inlink contribution to it 
	// or page rank value of parent depending on key's matrix name
	
	public ValueForMRComputation(Long outlinksPageIndex,
			Double pageRankContributionValue) {
		super();
		this.outlinksPageIndex = outlinksPageIndex;
		this.pageRankContributionValue = pageRankContributionValue;
	}
	
	// Default constructor
	public ValueForMRComputation() {
		super();
		this.outlinksPageIndex = 0l;
		this.pageRankContributionValue = 0.0;
	}

	
	// List of setters and getters
	public Long getOutlinksPageIndex() {
		return outlinksPageIndex;
	}

	public Double getPageRankContributionValue() {
		return pageRankContributionValue;
	}

	public void setOutlinksPageIndex(Long outlinksPageIndex) {
		this.outlinksPageIndex = outlinksPageIndex;
	}

	public void setPageRankContributionValue(Double pageRankContributionValue) {
		this.pageRankContributionValue = pageRankContributionValue;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		this.outlinksPageIndex = in.readLong();
		this.pageRankContributionValue = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		out.writeLong(outlinksPageIndex);
		out.writeDouble(pageRankContributionValue);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((outlinksPageIndex == null) ? 0 : outlinksPageIndex
						.hashCode());
		result = prime
				* result
				+ ((pageRankContributionValue == null) ? 0
						: pageRankContributionValue.hashCode());
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
		ValueForMRComputation other = (ValueForMRComputation) obj;
		if (outlinksPageIndex == null) {
			if (other.outlinksPageIndex != null)
				return false;
		} else if (!outlinksPageIndex.equals(other.outlinksPageIndex))
			return false;
		if (pageRankContributionValue == null) {
			if (other.pageRankContributionValue != null)
				return false;
		} else if (!pageRankContributionValue
				.equals(other.pageRankContributionValue))
			return false;
		return true;
	}
	
	
	

}
