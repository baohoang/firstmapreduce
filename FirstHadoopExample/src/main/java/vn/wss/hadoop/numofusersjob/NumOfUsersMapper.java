package vn.wss.hadoop.numofusersjob;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import vn.wss.hadoop.model.ListLongWritable;

public class NumOfUsersMapper extends MapReduceBase implements
		Mapper<LongWritable, ListLongWritable, LongWritable, IntWritable> {

	public void map(LongWritable arg0, ListLongWritable arg1,
			OutputCollector<LongWritable, IntWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		for (Long lw : arg1.getList()) {
			arg2.collect(new LongWritable(lw), new IntWritable(1));
		}
	}

}
