package vn.wss.hadoop.basejob;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataMapper extends MapReduceBase
		implements
		Mapper<ByteBuffer, SortedMap<ByteBuffer, BufferCell>, LongWritable, LongWritable> {

	private static final Logger logger = LogManager.getLogger(DataMapper.class);

	public void map(ByteBuffer keys, SortedMap<ByteBuffer, BufferCell> columns,
			OutputCollector<LongWritable, LongWritable> context, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		// String uri = null;
		// String useridText = null;
		logger.info("read a row with key: " + ByteBufferUtil.toInt(keys));
		logger.info("read: " + columns.size());
		int count = 0;
		long userID = -1;
		long itemID = -1;
		for (Entry<ByteBuffer, BufferCell> e : columns.entrySet()) {
			count++;
			BufferCell cell = e.getValue();
			ByteBuffer val = cell.value();
			if (count % 6 == 3) {
				itemID = getItemID(ByteBufferUtil.string(val));
				logger.info("uri: " + ByteBufferUtil.string(val));
			}
			if (count % 6 == 5) {
				logger.info("userID: " + ByteBufferUtil.string(val));
				userID = getUserID(ByteBufferUtil.string(val));
				if (userID != -1 && itemID != -1) {
					context.collect(new LongWritable(userID), new LongWritable(
							itemID));
				}
			}
		}
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
