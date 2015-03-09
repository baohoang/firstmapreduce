package vn.wss.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class ListLongWritable implements Writable {

	private List<LongWritable> list;
	private IntWritable size;

	public ListLongWritable() {
		list = new ArrayList<LongWritable>();
		size = new IntWritable(0);
	}

	public ListLongWritable(List<LongWritable> list) {
		this.list = list;
		this.size = new IntWritable(list.size());
	}

	public ListLongWritable(List<LongWritable> list, IntWritable size) {
		this.list = list;
		this.size = size;
	}

	public List<LongWritable> getList() {
		return list;
	}

	public void setList(List<LongWritable> list) {
		this.list = list;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		size.write(out);
		for (LongWritable lw : list) {
			lw.write(out);
		}
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		size.readFields(in);
		for (int i = 0; i < size.get(); i++) {
			LongWritable lw = new LongWritable();
			lw.readFields(in);
			list.add(lw);
		}
	}

	public void add(LongWritable lw) {
		list.add(lw);
	}

	public int size() {
		return size.get();
	}

	public LongWritable get(int index) {
		return list.get(index);
	}

	public void clear() {
		list.clear();
		size = new IntWritable(0);
	}
}
