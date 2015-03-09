package vn.wss.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class DataWritable implements Writable {
	private IntWritable n1, n2, n3;

	public DataWritable() {
	}

	public IntWritable getN1() {
		return n1;
	}

	public void setN1(IntWritable n1) {
		this.n1 = n1;
	}

	public IntWritable getN2() {
		return n2;
	}

	public void setN2(IntWritable n2) {
		this.n2 = n2;
	}

	public IntWritable getN3() {
		return n3;
	}

	public void setN3(IntWritable n3) {
		this.n3 = n3;
	}

	public DataWritable(IntWritable n1, IntWritable n2, IntWritable n3) {
		this.n1 = n1;
		this.n2 = n2;
		this.n3 = n3;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		n1.write(out);
		n2.write(out);
		n3.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		n1.readFields(in);
		n2.readFields(in);
		n3.readFields(in);
	}

}
