package com.manel.belhadj.kafka;

import java.util.Date;

public class Connection {
	
	private Date localDate;
	private String url;
	
	public Date getLocalDate() {
		return localDate;
	}
	public void setLocalDate(Date localDate) {
		this.localDate = localDate;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	
	public String toString(Date localDate, String url) {
		return  this.url + "-" + this.localDate.toString();
		
	}

}
