package vn.wss.hadoop.basejob;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataMapper extends MapReduceBase
		implements
		Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, LongWritable, LongWritable> {

	private static final Logger logger = LoggerFactory
			.getLogger(DataMapper.class);

	public void map(Map<String, ByteBuffer> keys,
			Map<String, ByteBuffer> columns,
			OutputCollector<LongWritable, LongWritable> context, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		String uri = null;
		String useridText = null;
		logger.info("starting ...");
		for (Entry<String, ByteBuffer> e : columns.entrySet()) {

			String value = ByteBufferUtil.string(e.getValue());
			logger.debug("read {}:{}={} from {}", new Object[] {
					toString(keys), e.getKey(), value, arg3.getInputSplit() });
			if ("uri".equalsIgnoreCase(e.getKey())) {
				uri = ByteBufferUtil.string(e.getValue());
			}
			if ("userid".equalsIgnoreCase(e.getKey())) {
				useridText = ByteBufferUtil.string(e.getValue());
			}
		}
		long userID = getUserID(useridText);
		long itemID = getItemID(uri);
		if (userID != -1 && itemID != -1) {
			context.collect(new LongWritable(userID), new LongWritable(itemID));
		}
	}

	private String toString(Map<String, ByteBuffer> keys) {
		String result = "";
		try {
			for (ByteBuffer key : keys.values())
				result = result + ByteBufferUtil.string(key) + ":";
		} catch (CharacterCodingException e) {
			logger.error("Failed to print keys", e);
		}
		return result;
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
