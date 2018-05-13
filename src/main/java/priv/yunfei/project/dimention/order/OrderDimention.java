package com.oracle.project.dimention.order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderDimention implements WritableComparable<OrderDimention> {

	private String date;
	private String sign;
	private String e_cs;
	private String e_cr;
	private String oid;
	
	public String getOid() {
		return oid;
	}

	public void setOid(String oid) {
		this.oid = oid;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getSign() {
		return sign;
	}

	public void setSign(String sign) {
		this.sign = sign;
	}

	public String getE_cs() {
		return e_cs;
	}

	public void setE_cs(String e_cs) {
		this.e_cs = e_cs;
	}

	public String getE_cr() {
		return e_cr;
	}

	public void setE_cr(String e_cr) {
		this.e_cr = e_cr;
	}

	public OrderDimention() {

	}
	
	public OrderDimention(String date, String sign, String e_cs, String e_cr,String oid) {
		this.date = date;
		this.sign = sign;
		this.e_cs = e_cs;
		this.e_cr = e_cr;
		this.oid = oid;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(date);
		out.writeUTF(sign);
		out.writeUTF(e_cs);
		out.writeUTF(e_cr);
		out.writeUTF(oid);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.date = in.readUTF();
		this.sign = in.readUTF();
		this.e_cs = in.readUTF();
		this.e_cr = in.readUTF();
		this.oid = in.readUTF();
	}

	@Override
	public int compareTo(OrderDimention o) {
		if(this == o){
			return 0;
		}
		
		int tmp = this.date.compareTo(o.date);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.sign.compareTo(o.sign);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.e_cs.compareTo(o.e_cs);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.e_cr.compareTo(o.e_cr);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.oid.compareTo(o.oid);
		if(tmp != 0){
			return tmp;
		}
		
		return 0;
	}
	
	@Override
	public int hashCode() {
		int result = 1;
		int prime = 100;
		
		result = (result*prime) + this.date == null ? 0 : this.date.hashCode();
		result = (result*prime) + this.sign == null ? 0 : this.sign.hashCode();
		result = (result*prime) + this.e_cs == null ? 0 : this.e_cs.hashCode();
		result = (result*prime) + this.e_cr == null ? 0 : this.e_cr.hashCode();
		result = (result*prime) + this.oid == null ? 0 : this.oid.hashCode();
		
		return result;
	}
	
	
	
	@Override
	public boolean equals(Object obj) {
		
		if(this == obj){
			return true;
		}
		
		if(this.getClass() != obj.getClass()){
			return false;
		}
		
		OrderDimention orderDimention = (OrderDimention) obj;
		
		if(this.date == null){
			if(orderDimention.date != null){
				return false;
			}
		}else if(orderDimention.date == null) {
			if(this.date != null){
				return false;
			}
		}else if(!this.date.equals(orderDimention.date)) {
			return false;
		}
		
		if(this.sign == null){
			if(orderDimention.sign != null){
				return false;
			}
		}else if(orderDimention.sign == null) {
			if(this.sign != null){
				return false;
			}
		}else if(!this.sign.equals(orderDimention.sign)) {
			return false;
		}
		
		if(this.e_cs == null){
			if(orderDimention.e_cs != null){
				return false;
			}
		}else if(orderDimention.e_cs == null) {
			if(this.e_cs != null){
				return false;
			}
		}else if(!this.e_cs.equals(orderDimention.e_cs)) {
			return false;
		}
		
		if(this.e_cr == null){
			if(orderDimention.e_cr != null){
				return false;
			}
		}else if(orderDimention.e_cr == null) {
			if(this.e_cr != null){
				return false;
			}
		}else if(!this.e_cr.equals(orderDimention.e_cr)) {
			return false;
		}
		
		if(this.oid == null){
			if(orderDimention.oid != null){
				return false;
			}
		}else if(orderDimention.oid == null) {
			if(this.oid != null){
				return false;
			}
		}else if(!this.oid.equals(orderDimention.oid)) {
			return false;
		}
		
		return true;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.date + "\t" + this.sign + "\t" + this.e_cs + "\t" + this.e_cr + "\t" + this.oid;
	}
}
