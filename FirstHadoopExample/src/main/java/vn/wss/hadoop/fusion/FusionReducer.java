package vn.wss.hadoop.fusion;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import vn.wss.hadoop.model.DataWritable;
import vn.wss.hadoop.model.PairWritable;

public class FusionReducer extends MapReduceBase implements
		Reducer<PairWritable, DataWritable, PairWritable, DataWritable> {

	public void reduce(PairWritable arg0, Iterator<DataWritable> arg1,
			OutputCollector<PairWritable, DataWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		int maxN1 = Integer.MIN_VALUE;
		int maxN2 = Integer.MIN_VALUE;
		int maxN3 = Integer.MIN_VALUE;
		while (arg1.hasNext()) {
			DataWritable d = arg1.next();
			maxN1 = Math.max(maxN1, d.getN1().get());
			maxN2 = Math.max(maxN2, d.getN2().get());
			maxN3 = Math.max(maxN3, d.getN3().get());
		}
		IntWritable n1 = new IntWritable(maxN1);
		IntWritable n2 = new IntWritable(maxN2);
		IntWritable n3 = new IntWritable(maxN3);
		DataWritable val = new DataWritable(n1, n2, n3);
		arg2.collect(arg0, val);
	}

}
