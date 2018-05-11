package com.oracle.project.outputformat.order;

public class MapValue {
	
	private int successCount;
	private int refundCount;
	private String oidNumber;
	private int orderNumber;
	private double orderCount;
	
	public String getOidNumber() {
		return oidNumber;
	}
	public void setOidNumber(String oidNumber) {
		this.oidNumber = oidNumber;
	}
	
	public int getSuccessCount() {
		return successCount;
	}
	public void setSuccessCount(int successCount) {
		this.successCount = successCount;
	}
	public int getRefundCount() {
		return refundCount;
	}
	public void setRefundCount(int refundCount) {
		this.refundCount = refundCount;
	}
	
	public int getOrderNumber() {
		return orderNumber;
	}
	public void setOrderNumber(int orderNumber) {
		this.orderNumber = orderNumber;
	}
	public double getOrderCount() {
		return orderCount;
	}
	public void setOrderCount(double orderCount) {
		this.orderCount = orderCount;
	}
	
	public MapValue() {

	}
	
	public MapValue(int successCount, int refundCount, String oidNumber, int orderNumber, double orderCount) {
		super();
		this.successCount = successCount;
		this.refundCount = refundCount;
		this.oidNumber = oidNumber;
		this.orderNumber = orderNumber;
		this.orderCount = orderCount;
	}
	
	
}
