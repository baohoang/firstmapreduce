package vn.wss.hadoop.recommendation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import vn.wss.hadoop.model.ListLongWritable;
import vn.wss.hadoop.model.RatingWritable;

public class RecommendationReducer extends MapReduceBase implements
		Reducer<LongWritable, RatingWritable, LongWritable, ListLongWritable> {

	public void reduce(LongWritable arg0, Iterator<RatingWritable> arg1,
			OutputCollector<LongWritable, ListLongWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		SortedMap<Double, Long> sortedMap = new TreeMap<Double, Long>(
				new Comparator<Double>() {

					public int compare(Double o1, Double o2) {
						// TODO Auto-generated method stub
						return o2.compareTo(o1);
					}

				});
		while (arg1.hasNext()) {
			RatingWritable r = arg1.next();
			sortedMap.put(r.getRate().get(), r.getId().get());
		}
		List<Long> val = new ArrayList<Long>();
		Iterator<Entry<Double, Long>> iterator = sortedMap.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			if (val.size() == 10) {
				break;
			} else {
				Entry<Double, Long> entry = iterator.next();
				val.add(entry.getValue());
			}
		}
		arg2.collect(arg0, new ListLongWritable(val));
	}
}
