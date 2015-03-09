package vn.wss.hadoop.basejob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import vn.wss.hadoop.model.ListLongWritable;

public class DataReducer extends MapReduceBase implements
		Reducer<LongWritable, LongWritable, LongWritable, ListLongWritable> {

	public void reduce(LongWritable arg0, Iterator<LongWritable> arg1,
			OutputCollector<LongWritable, ListLongWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		List<LongWritable> arr = new ArrayList<LongWritable>();
		while (arg1.hasNext()) {
			arr.add(arg1.next());
		}
		ListLongWritable llw = new ListLongWritable(arr);
		arg2.collect(arg0, llw);
	}
}
