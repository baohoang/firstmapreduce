package vn.wss.hadoop.fusion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import vn.wss.hadoop.model.DataWritable;
import vn.wss.hadoop.model.PairWritable;
import vn.wss.hadoop.model.PrefWritable;

public class Fusion2Reducer extends MapReduceBase implements
		Reducer<LongWritable, PrefWritable, PairWritable, DataWritable> {

	public void reduce(LongWritable arg0, Iterator<PrefWritable> arg1,
			OutputCollector<PairWritable, DataWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		List<PrefWritable> list = new ArrayList<PrefWritable>();
		int val = -1;
		while (arg1.hasNext()) {
			PrefWritable e = arg1.next();
			if (e.getId() == -1) {
				val = e.getPref();
			}
			list.add(e);
		}
		for (int i = 0; i < list.size(); i++) {
			PrefWritable index = list.get(i);
			if (index.getId() != -1) {
				DataWritable value = new DataWritable(new IntWritable(
						index.getPref()), new IntWritable(-1), new IntWritable(
						val));
				PairWritable key = new PairWritable(index.getId(), arg0.get());
				arg2.collect(key, value);
			}
		}
	}

}
