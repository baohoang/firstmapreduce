package vn.wss.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.math.Varint;

import com.google.common.primitives.Longs;

public class PairWritable implements WritableComparable<PairWritable>,
		Cloneable {

	private long aID;
	private long bID;

	public PairWritable() {
	}

	public PairWritable(long aID, long bID) {
		this.aID = aID;
		this.bID = bID;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		Varint.writeSignedVarLong(aID, out);
		Varint.writeSignedVarLong(bID, out);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		aID = Varint.readSignedVarLong(in);
		bID = Varint.readSignedVarLong(in);
	}

	public long getaID() {
		return aID;
	}

	public void setaID(long aID) {
		this.aID = aID;
	}

	public long getbID() {
		return bID;
	}

	public void setbID(long bID) {
		this.bID = bID;
	}

	public int compareTo(PairWritable o) {
		// TODO Auto-generated method stub
		int res = compare(aID, o.getaID());
		return res == 0 ? compare(bID, o.getbID()) : 0;
	}

	public static int compare(long a, long b) {
		return a > b ? 1 : a < b ? -1 : 0;
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return new PairWritable(aID, bID);
	}

	@Override
	public boolean equals(Object arg0) {
		// TODO Auto-generated method stub
		if (arg0 instanceof PairWritable) {
			PairWritable o = (PairWritable) arg0;
			return aID == o.getaID() && bID == o.getbID();
		}
		return false;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return Longs.hashCode(aID) + Longs.hashCode(bID);
	}

}
