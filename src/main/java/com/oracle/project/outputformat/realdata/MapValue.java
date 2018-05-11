package com.oracle.project.outputformat.realdata;

public class MapValue {
	private String type = null;
	private Double amount = 0.0;
	private Integer count = 0;

	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public Double getAmount() {
		return amount;
	}
	public void setAmount(Double amount) {
		this.amount = amount;
	}
	public Integer getCount() {
		return count;
	}
	public void setCount(Integer count) {
		this.count = count;
	}
	
	public MapValue(String type) {
		this.type = type;
	}
}
