package vn.wss.copydata;

import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class CopyData {
	private static final Logger log = LogManager.getLogger(CopyData.class);
	private static CopyData instance = null;
	final String CONTACT_POINT = "192.168.100.33";
	final String KEYSPACE = "tracking";
	PreparedStatement statement;
	Cluster cluster;
	Session session;

	public static CopyData getInstance() {
		if (instance == null) {
			instance = new CopyData();
		}
		return instance;
	}

	BoundStatement boundStatement = null;

	public CopyData() {
		log.info("create new instance");
		connect();
		String cql = "INSERT INTO tracking (year_month, at, ip, referer, session_id, uri) VALUES (?,?,?,?,?,?);";
		statement = session.prepare(cql);
		boundStatement = new BoundStatement(statement);
	}

	void connect() {
		cluster = Cluster.builder().addContactPoint(CONTACT_POINT).build();
		log.info("Hosts: " + cluster.getMetadata().getAllHosts());
		session = cluster.connect(KEYSPACE);
	}

	void writeData(Tracking tracking) {
		session.execute(boundStatement.bind(tracking.getYear_month(),
				tracking.getAt(), tracking.getIp(), tracking.getReferer(),
				tracking.getSessionId(), tracking.getUri()));
	}

	public void copyData(List<Tracking> list) {
		for (int i = 0; i < list.size(); i++) {
			writeData(list.get(i));
		}
		// session.close();
		// cluster.close();
	}

	public void close() {
		if (instance != null) {
			instance = null;
		}
		session.close();
		cluster.close();
	}

	public static void main(String[] args) {
		int month = Integer.parseInt(args[0]);
		Date d = new DateTime(2015, month, 0, 0, 0).toDate();
		int year_month = 2015 * 100 + month;
		int limit = 100;
		List<Tracking> res = CassandraDB.getInstance().getTrackings(year_month,
				d, limit);
		while (res.size() > 0) {
			CopyData.getInstance().copyData(res);
			d = res.get(res.size() - 1).getAt();
			res = CassandraDB.getInstance().getTrackings(year_month, d, limit);
		}
		CassandraDB.getInstance().close();
		CopyData.getInstance().close();
	}
}
