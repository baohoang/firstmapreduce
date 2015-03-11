package vn.wss.hadoop.basejob;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.db.Cell;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataMapper extends MapReduceBase
		implements
		Mapper<ByteBuffer, SortedMap<ByteBuffer, Cell>, LongWritable, LongWritable> {

	private static final Logger logger = LogManager
			.getLogger(DataMapper.class);

	public void map(ByteBuffer keys, SortedMap<ByteBuffer, Cell> columns,
			OutputCollector<LongWritable, LongWritable> context, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		// String uri = null;
		// String useridText = null;
		 logger.info("keys: "+keys.toString());
		for (Entry<ByteBuffer, Cell> e : columns.entrySet()) {
			String key = e.getKey().toString();
			Cell column = e.getValue();
			System.out.print("a1");
			logger.info("colum: " + key + " " + column.toString());
			// if ("uri".equalsIgnoreCase(e.getKey())) {
			// uri = ByteBufferUtil.string(e.getValue());
			// }
			// if ("userid".equalsIgnoreCase(e.getKey())) {
			// useridText = ByteBufferUtil.string(e.getValue());
			// }
		}
		// long userID = getUserID(useridText);
		// long itemID = getItemID(uri);
		// if (userID != -1 && itemID != -1) {
		// context.collect(new LongWritable(userID), new LongWritable(itemID));
		// }
		context.collect(new LongWritable(1), new LongWritable(1));
	}

	public long getUserID(String s) {
		String regex = "\\d+";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(s);
		boolean flag = matcher.matches();
		if (flag) {
			long userId = Long.parseLong(s);
			return userId;
		}
		return -1;
	}

	public long getItemID(String uri) {
		String regex = ".*\\/(\\d+)\\/(so-sanh.htm)";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(uri);
		boolean flag = matcher.matches();
		if (flag) {
			String itemIdstr = matcher.group(1);
			long itemID = Long.valueOf(itemIdstr);
			return itemID;
		}
		return -1;
	}

}
