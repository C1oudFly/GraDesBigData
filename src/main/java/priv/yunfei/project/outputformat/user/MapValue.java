package com.oracle.project.outputformat.user;

public class MapValue {
	
	private int adduserCount = 0;
	private int visitCount = 0;
	private int userCount = 0;
	private int addmemberCount = 0;
	private int activeCount = 0;
	private int memberCount = 0;
	private int sessionnumberCount = 0;
	private double sessionlengthCount = 0;
	private double averagesessionlength = 0;
	
	public int getAdduserCount() {
		return adduserCount;
	}

	public void setAdduserCount(int adduserCount) {
		this.adduserCount = adduserCount;
	}

	public int getVisitCount() {
		return visitCount;
	}

	public void setVisitCount(int visitCount) {
		this.visitCount = visitCount;
	}

	public int getUserCount() {
		return userCount;
	}

	public void setUserCount(int userCount) {
		this.userCount = userCount;
	}

	public int getAddmemberCount() {
		return addmemberCount;
	}

	public void setAddmemberCount(int addmemberCount) {
		this.addmemberCount = addmemberCount;
	}

	public int getActiveCount() {
		return activeCount;
	}

	public void setActiveCount(int activeCount) {
		this.activeCount = activeCount;
	}

	public int getMemberCount() {
		return memberCount;
	}

	public void setMemberCount(int memberCount) {
		this.memberCount = memberCount;
	}

	public int getSessionnumberCount() {
		return sessionnumberCount;
	}

	public void setSessionnumberCount(int sessionnumberCount) {
		this.sessionnumberCount = sessionnumberCount;
	}

	public double getSessionlengthCount() {
		return sessionlengthCount;
	}

	public void setSessionlengthCount(double sessionlengthCount) {
		this.sessionlengthCount = sessionlengthCount;
	}

	public double getAveragesessionlength() {
		return averagesessionlength;
	}

	public void setAveragesessionlength(double averagesessionlength) {
		this.averagesessionlength = averagesessionlength;
	}

	public MapValue() {

	}
	
	public MapValue(String sign, int adduserCount, int visitCount,
			int memberCount, int activeCount, int sessionnumberCount, double sessionlengthCount) {
		this.adduserCount = adduserCount;
		this.visitCount = visitCount;
		this.memberCount = memberCount;
		this.activeCount = activeCount;
		this.sessionnumberCount = sessionnumberCount;
		this.sessionlengthCount = sessionlengthCount;
	}

}
