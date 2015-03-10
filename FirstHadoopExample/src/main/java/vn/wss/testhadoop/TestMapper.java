package vn.wss.testhadoop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.SortedMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TestMapper extends MapReduceBase implements Mapper<Map<String,ByteBuffer>, Map<String, ByteBuffer>, LongWritable, LongWritable>{

	public void map(Map<String, ByteBuffer> arg0, Map<String, ByteBuffer> arg1,
			OutputCollector<LongWritable, LongWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

}
