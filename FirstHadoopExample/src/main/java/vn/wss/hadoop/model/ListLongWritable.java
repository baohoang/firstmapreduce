package vn.wss.hadoop.model;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Arrays;

public class ListLongWritable extends ArrayWritable {

	public ListLongWritable(Class<? extends Writable> valueClass,
			Writable[] values) {
		super(valueClass, values);
	}

	public ListLongWritable(Class<? extends Writable> valueClass) {
		super(valueClass);
	}

	@Override
	public LongWritable[] get() {
		return (LongWritable[]) super.get();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		super.write(arg0);
	}

	public int size() {
		return get().length;
	}

	public LongWritable get(int index) {
		return get()[index];
	}

	public String toString() {
		// TODO Auto-generated method stub
		return Arrays.toString(get());
	}
	

}
