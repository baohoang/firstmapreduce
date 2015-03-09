package vn.wss.copydata;
import java.util.Date;

public class Tracking {
	Date at;
	String ip;
	String referer;
	String sessionId;
	String uri;
	String userId;
	int year_month;
	public Tracking(int year_month,Date at, String ip, String referer, String sessionId,
			String uri, String userId) {
		super();
		this.year_month=year_month;
		this.at = at;
		this.ip = ip;
		this.referer = referer;
		this.sessionId = sessionId;
		this.uri = uri;
		this.userId = userId;
	}
	@Override
	public String toString() {
		return String.format("tracking{year_month= %d, at = %s, ip = %s, uri = %s, user_id = %s }", year_month,at, ip, uri, userId);
	}
	
	public int getYear_month() {
		return year_month;
	}
	public void setYear_month(int year_month) {
		this.year_month = year_month;
	}
	public Date getAt() {
		return at;
	}
	public void setAt(Date at) {
		this.at = at;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getReferer() {
		return referer;
	}
	public void setReferer(String referer) {
		this.referer = referer;
	}
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	
}
