package vn.wss.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class ListLongWritable implements Writable{

	private List<LongWritable> list;

	public ListLongWritable() {
		list = new ArrayList<LongWritable>();
	}

	public ListLongWritable(List<LongWritable> list) {
		this.list = list;
	}

	public List<LongWritable> getList() {
		return list;
	}

	public void setList(List<LongWritable> list) {
		this.list = list;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(list.size());
		for (int i = 0; i < list.size(); i++) {
			list.get(i).write(out);
		}
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int size = in.readInt();
		list = new ArrayList<LongWritable>(size);
		for (int i = 0; i < size; i++) {
			LongWritable l=new LongWritable();
			l.readFields(in);
			list.add(l);
		}
	}

	public void add(LongWritable lw) {
		list.add(lw);
	}

	public int size() {
		return list.size();
	}

	public LongWritable get(int index) {
		return list.get(index);
	}

	public void clear() {
		list.clear();
	}

}
