package vn.wss.hadoop.fusion;

import java.io.IOException;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import vn.wss.hadoop.model.DataWritable;
import vn.wss.hadoop.model.PairWritable;

public class FusionMapper extends MapReduceBase implements Mapper<PairWritable, DataWritable, PairWritable, DataWritable> {

	public void map(PairWritable arg0, DataWritable arg1,
			OutputCollector<PairWritable, DataWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		arg2.collect(arg0, arg1);
	}

}
