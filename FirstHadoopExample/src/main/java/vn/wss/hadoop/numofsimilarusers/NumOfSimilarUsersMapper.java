package vn.wss.hadoop.numofsimilarusers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import vn.wss.hadoop.model.ListLongWritable;
import vn.wss.hadoop.model.PairWritable;

public class NumOfSimilarUsersMapper extends MapReduceBase implements
		Mapper<LongWritable, ListLongWritable, PairWritable, IntWritable> {

	public void map(LongWritable arg0, ListLongWritable arg1,
			OutputCollector<PairWritable, IntWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		for (int i = 0; i < arg1.size(); i++) {
			for (int j = i + 1; j < arg1.size(); j++) {
				long aID = arg1.get(i);
				long bID = arg1.get(j);
				PairWritable e1 = new PairWritable(aID, bID);
				PairWritable e2 = new PairWritable(bID, aID);
				arg2.collect(e1, new IntWritable(1));
				arg2.collect(e2, new IntWritable(1));
			}

		}
	}

}
