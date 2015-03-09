package vn.wss.copydata;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraDB {
	private static final Logger log = LogManager.getLogger(CassandraDB.class);
	private static CassandraDB instance = null;
	static Object syncRoot = new Object();
	Cluster cluster;
	Session session;
	final String CONTACT_POINT_1 = "10.0.0.11";
	final String CONTACT_POINT_2 = "10.0.0.12";
	final String CONTACT_POINT_3 = "10.0.0.13";
	final String CONTACT_POINT_4 = "10.0.0.14";
	final String KEYSPACE = "tracking";

	PreparedStatement getTracking;

	public static CassandraDB getInstance() {
		if (instance == null) {
			instance = new CassandraDB();
		}
		return instance;
	}

	private CassandraDB() {
		log.info("create new instance");
		connect();
		init();
		// initTestDB();
	}

	void connect() {
		cluster = Cluster.builder().addContactPoint(CONTACT_POINT_1)
				.addContactPoint(CONTACT_POINT_2)
				.addContactPoint(CONTACT_POINT_3)
				.addContactPoint(CONTACT_POINT_4).build();
		// log.info("Connected to cluster " + cluster.getClusterName());
		log.info("Hosts: " + cluster.getMetadata().getAllHosts());
		session = cluster.connect(KEYSPACE);
	}

	void init() {
		String cql = "SELECT * FROM tracking WHERE year_month = ? AND at > ? LIMIT ?;";
		getTracking = session.prepare(cql);
	}

	void initTestDB() {
		String cql = "SELECT * FROM tracking LIMIT ?";
		getTracking = session.prepare(cql);
	}

	public List<Tracking> getResult(int limit) {
		List<Tracking> res = new ArrayList<Tracking>();
		ResultSet resultSet = session.execute(getTracking.bind(limit));
		for (Row row : resultSet) {
			log.info(row.toString());
			int year_month = row.getInt("year_month");
			Date at = row.getDate("at");
			String ip = row.getString("ip");
			String referer = row.getString("referer");
			String sessionId = row.getString("session_id");
			String uri = row.getString("uri");
			String userId = row.getString("user_id");
			res.add(new Tracking(year_month, at, ip, referer, sessionId, uri,
					userId));
		}
		session.close();
		cluster.close();
		return res;
	}

	public void getAllTrackings(Date fromTime, Date toTime) {
		ResultSet resultSet = session.execute(getTracking.bind(fromTime,
				toTime, 1000));
		System.out.println(resultSet.all().size());
	}

	public List<Tracking> getTrackings(int year_month, Date from, int limit) {
		List<Tracking> result = new ArrayList<Tracking>();
		ResultSet resultSet = session.execute(getTracking.bind(year_month,
				from, limit));
		for (Row row : resultSet) {
			// log.info(row.toString());
			Date at = row.getDate("at");
			String ip = row.getString("ip");
			String referer = row.getString("referer");
			String sessionId = row.getString("session_id");
			String uri = row.getString("uri");
			String userId = row.getString("user_id");
			result.add(new Tracking(year_month, at, ip, referer, sessionId,
					uri, userId));
		}
		// session.close();
		// cluster.close();
		return result;
	}

	public void close() {
		if (instance != null) {
			instance = null;
		}
		session.close();
		cluster.close();
	}

}
