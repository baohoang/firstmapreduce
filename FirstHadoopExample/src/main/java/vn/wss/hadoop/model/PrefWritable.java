package vn.wss.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.math.Varint;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public class PrefWritable implements WritableComparable<PrefWritable>,
		Cloneable {
	private long id;
	private int pref;

	public PrefWritable() {
	}

	public PrefWritable(long id, int pref) {
		this.id = id;
		this.pref = pref;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public int getPref() {
		return pref;
	}

	public void setPref(int pref) {
		this.pref = pref;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		Varint.writeSignedVarLong(id, out);
		Varint.writeSignedVarInt(pref, out);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id = Varint.readSignedVarLong(in);
		pref = Varint.readSignedVarInt(in);
	}

	public int compareTo(PrefWritable o) {
		// TODO Auto-generated method stub
		int res = compareLong(id, o.getId());
		return res == 0 ? compateInt(pref, o.getPref()) : res;
	}

	private static int compareLong(long a, long b) {
		return a < b ? -1 : a > b ? 1 : 0;
	}

	private static int compateInt(int a, int b) {
		return a < b ? -1 : a > b ? 1 : 0;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return Longs.hashCode(id) + Ints.hashCode(pref);
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return new PrefWritable(id, pref);
	}

	@Override
	public boolean equals(Object o) {
		// TODO Auto-generated method stub
		if (o instanceof PrefWritable) {
			PrefWritable that = (PrefWritable) o;
			return id == that.getId() && pref == that.getPref();
		}
		return false;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return id + " " + pref;
	}

}
