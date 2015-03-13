package vn.wss.hadoop.basejob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

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
		ArrayList<LongWritable> arr = new ArrayList<LongWritable>();
		// long res = 0;
		while (arg1.hasNext()) {
			// res += arg1.next().get();
			arr.add(arg1.next());
		}
		arg2.collect(
				arg0,
				new ListLongWritable(LongWritable.class, arr
						.toArray(new LongWritable[arr.size()])));
	}
}
