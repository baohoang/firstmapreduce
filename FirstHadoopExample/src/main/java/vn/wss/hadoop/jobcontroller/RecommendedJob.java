package vn.wss.hadoop.jobcontroller;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;

import vn.wss.hadoop.numofusersjob.NumOfUsersMapper;
import vn.wss.hadoop.numofusersjob.NumOfUsersReducer;

public class RecommendedJob extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf(getConf(), RecommendedJob.class);
		conf.setJobName("Logging");

		// Setting configuration object with the Data Type of output Key and
		// Value
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(LongWritable.class);

		// Providing the mapper and reducer class names
		conf.setMapperClass(NumOfUsersMapper.class);
		conf.setReducerClass(NumOfUsersReducer.class);
		// We wil give 2 arguments at the run time, one in input path and other
		// is output path
		Path inp = new Path(args[0]);
		Path out = new Path(args[1]);
		// the hdfs input and output directory to be fetched from the command
		// line
		FileInputFormat.setInputPaths(conf,inp);
		FileOutputFormat.setOutputPath(conf, out);
		JobClient.runJob(conf);
		return 0;
	}

}
