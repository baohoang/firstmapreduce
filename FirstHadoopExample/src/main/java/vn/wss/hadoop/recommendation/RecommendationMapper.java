package vn.wss.hadoop.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import vn.wss.hadoop.model.DataWritable;
import vn.wss.hadoop.model.PairWritable;
import vn.wss.hadoop.model.RatingWritable;

public class RecommendationMapper extends MapReduceBase implements
		Mapper<PairWritable, DataWritable, LongWritable, RatingWritable> {

	public void map(PairWritable arg0, DataWritable arg1,
			OutputCollector<LongWritable, RatingWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		int n1 = arg1.getN1().get();
		int n2 = arg1.getN2().get();
		int n3 = arg1.getN3().get();
		double val = n1 / (n2 + n3 - n1);
		arg2.collect(new LongWritable(arg0.getaID()), new RatingWritable(
				new LongWritable(arg0.getbID()), new DoubleWritable(val)));
	}
}
