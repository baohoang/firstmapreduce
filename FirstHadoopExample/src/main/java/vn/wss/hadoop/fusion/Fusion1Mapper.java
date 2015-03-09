package vn.wss.hadoop.fusion;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import vn.wss.hadoop.model.PairWritable;
import vn.wss.hadoop.model.PrefWritable;

public class Fusion1Mapper extends MapReduceBase implements
		Mapper<PairWritable, IntWritable, LongWritable, PrefWritable> {

	public void map(PairWritable arg0, IntWritable arg1,
			OutputCollector<LongWritable, PrefWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		LongWritable key = new LongWritable(arg0.getaID());
		PrefWritable pref = new PrefWritable(arg0.getbID(), arg1.get());
		arg2.collect(key, pref);
	}

}
