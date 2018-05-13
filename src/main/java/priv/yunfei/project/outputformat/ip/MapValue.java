package com.oracle.project.outputformat.ip;

public class MapValue {
	private String key;
	private int region = 0;
	private int sessionregion = 0;
	private int sessionjumpnumber = 0;
	private double jumprate = 0;
	
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public int getRegion() {
		return region;
	}

	public void setRegion(int region) {
		this.region = region;
	}

	public int getSessionregion() {
		return sessionregion;
	}

	public void setSessionregion(int sessionregion) {
		this.sessionregion = sessionregion;
	}

	public int getSessionjumpnumber() {
		return sessionjumpnumber;
	}

	public void setSessionjumpnumber(int sessionjumpnumber) {
		this.sessionjumpnumber = sessionjumpnumber;
	}

	public double getJumprate() {
		return jumprate;
	}

	public void setJumprate(double jumprate) {
		this.jumprate = jumprate;
	}
	
	public MapValue() {

	}
	
	public MapValue(String key) {
		this.key = key;
	}
	
	public MapValue(String key, int region, int sessionregion, int sessionjumpnumber, double jumprate) {
		this.key = key;
		this.region = region;
		this.sessionregion = sessionregion;
		this.sessionjumpnumber = sessionjumpnumber;
		this.jumprate = jumprate;
	}
	
}
