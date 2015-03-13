package vn.wss.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ListLongWritable implements Comparable<ListLongWritable>,
		Cloneable {

	private List<Long> list;

	public ListLongWritable() {
		list = new ArrayList<Long>();
	}

	public ListLongWritable(List<Long> list) {
		this.list = list;
	}

	public List<Long> getList() {
		return list;
	}

	public void setList(List<Long> list) {
		this.list = list;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(list.size());
		for (int i = 0; i < list.size(); i++) {
			out.writeLong(list.get(i));
		}
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int size = in.readInt();
		list = new ArrayList<Long>();
		for (int i = 0; i < size; i++) {
			list.add(in.readLong());
		}
	}

	public void add(long lw) {
		list.add(lw);
	}

	public int size() {
		return list.size();
	}

	public long get(int index) {
		return list.get(index);
	}

	public void clear() {
		list.clear();
	}

	public int compareTo(ListLongWritable o) {
		// TODO Auto-generated method stub
		if (list.size() == o.size()) {
			for (int i = 0; i < list.size(); i++) {
				if (list.get(i) == o.get(i)) {

				} else {
					return list.get(i) > o.get(i) ? 1 : -1;
				}
			}
			return 0;
		} else {
			return list.size() > o.size() ? 1 : -1;
		}
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return new ListLongWritable();
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		int prime = 31;
		int result = 1;
		result = prime * result + Integer.hashCode(list.size());
		for (int i = 0; i < list.size(); i++) {
			result = prime * result + Long.hashCode(list.get(i));
		}
		return result;
	}

}
