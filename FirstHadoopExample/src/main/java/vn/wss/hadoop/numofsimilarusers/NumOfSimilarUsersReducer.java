package vn.wss.hadoop.numofsimilarusers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import vn.wss.hadoop.model.PairWritable;

public class NumOfSimilarUsersReducer extends MapReduceBase implements
		Reducer<PairWritable, IntWritable, PairWritable, IntWritable> {

	public void reduce(PairWritable arg0, Iterator<IntWritable> arg1,
			OutputCollector<PairWritable, IntWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		int count = 0;
		while (arg1.hasNext()) {
			count += arg1.next().get();
		}
		arg2.collect(arg0, new IntWritable(count));
	}
}
