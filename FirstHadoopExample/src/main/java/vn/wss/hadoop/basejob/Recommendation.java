package vn.wss.hadoop.basejob;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Recommendation extends Configuration implements Tool {
	private final static String KEYSPACE = "tracking";
	private final static String COLUMN_FAMILY = "tracking";

	private final static String OUTPUT_LOCATION = "/recommendation/basejob";

	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub

	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return new Configuration();
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf(getConf(), Recommendation.class);
		conf.setJarByClass(Recommendation.class);
		conf.setJobName("Recommendation");

		// Setting configuration object with the Data Type of output Key and
		// Value
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(LongWritable.class);

		// Providing the mapper and reducer class names
		conf.setMapperClass(DataMapper.class);
		conf.setReducerClass(DataReducer.class);

		// We wil give 2 arguments at the run time, one in input path and other
		// is output path

		// Path inp = new Path(args[0]);

		// setup Input Cassandra
		ConfigHelper.setInputInitialAddress(conf, args[0]);
		ConfigHelper.setInputColumnFamily(conf, KEYSPACE, COLUMN_FAMILY);
		ConfigHelper.setInputPartitioner(conf,
				Murmur3Partitioner.class.getName());
		conf.setInputFormat(ColumnFamilyInputFormat.class);
		SlicePredicate predicate = new SlicePredicate()
				.setSlice_range(new SliceRange()
						.setStart(ByteBufferUtil.EMPTY_BYTE_BUFFER)
						.setFinish(ByteBufferUtil.EMPTY_BYTE_BUFFER)
						.setCount(Integer.MAX_VALUE));
		ConfigHelper.setInputSlicePredicate(conf, predicate);
		CqlConfigHelper.setInputCQLPageRowSize(conf, "3");

		// setup output Hadoop
		// the hdfs input and output directory to be fetched from the command
		// line
		// FileInputFormat.setInputPaths(conf,inp);

		// It will delete the output directory if it already exists. don't need
		// to delete it manually
		Path out = new Path(OUTPUT_LOCATION);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(out, true);

		// set output
		FileOutputFormat.setOutputPath(conf, out);
		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) {
		int res = 0;
		try {
			res = ToolRunner.run(new Configuration(), new Recommendation(),
					args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(res);
	}

}
