package vn.wss.hadoop.numofusersjob;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import vn.wss.hadoop.model.PairWritable;

public class NumOfUsersReducer extends MapReduceBase implements
		Reducer<LongWritable, IntWritable, PairWritable, IntWritable> {

	public void reduce(LongWritable arg0, Iterator<IntWritable> arg1,
			OutputCollector<PairWritable, IntWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		int count = 0;
		while (arg1.hasNext()) {
			count += arg1.next().get();
		}
		PairWritable arg = new PairWritable(arg0.get(), -1);
		arg2.collect(arg, new IntWritable(count));
	}
}
