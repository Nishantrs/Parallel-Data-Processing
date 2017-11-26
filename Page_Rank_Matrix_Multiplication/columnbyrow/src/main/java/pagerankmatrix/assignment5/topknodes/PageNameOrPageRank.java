package pagerankmatrix.assignment5.topknodes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


// Custom value class to find whether the record is being received from
// R-Matrix or Index Mapping Matrix
public class PageNameOrPageRank implements Writable{
	
	private String pageName;
	private Double pageRank;
	private Boolean isName;

	// Default constructor
	public PageNameOrPageRank() {
		super();
		this.pageName = new String();
		this.pageRank = 0.0;
		this.isName = false;
	}

	// This constructor is used to determine whether record contains page rank value or page name
	public PageNameOrPageRank(String pageName, Double pageRank, Boolean isName) {
		super();
		this.pageName = pageName;
		this.pageRank = pageRank;
		this.isName = isName;
	}

	// List of setters and getters
	public String getPageName() {
		return pageName;
	}

	public Double getPageRank() {
		return pageRank;
	}

	public Boolean getIsName() {
		return isName;
	}

	public void setPageName(String pageName) {
		this.pageName = pageName;
	}

	public void setPageRank(Double pageRank) {
		this.pageRank = pageRank;
	}

	public void setIsName(Boolean isName) {
		this.isName = isName;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub		
		this.pageName = in.readUTF();
		this.pageRank = in.readDouble();
		this.isName = in.readBoolean();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(pageName);
		out.writeDouble(pageRank);
		out.writeBoolean(isName);
	}

}
