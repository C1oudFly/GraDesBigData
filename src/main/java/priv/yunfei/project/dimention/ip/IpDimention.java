package com.oracle.project.dimention.ip;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IpDimention implements WritableComparable<IpDimention> {
	private String sign;
	private String date;
	private String ip;
	private String u_sd;
	
	
	public String getU_sd() {
		return u_sd;
	}

	public void setU_sd(String u_sd) {
		this.u_sd = u_sd;
	}

	public String getSign() {
		return sign;
	}

	public void setSign(String sign) {
		this.sign = sign;
	}
	
	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public IpDimention() {

	}
	
	public IpDimention(String sign, String date, String ip, String u_sd) {
		this.sign = sign;
		this.date = date;
		this.ip = ip;
		this.u_sd = u_sd;
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(sign);
		out.writeUTF(date);
		out.writeUTF(ip);
		out.writeUTF(u_sd);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.sign = in.readUTF();
		this.date = in.readUTF();
		this.ip = in.readUTF();
		this.u_sd = in.readUTF();
	}

	@Override
	public int compareTo(IpDimention o) {
		if(this == o){
			return 0;
		}
		
		int tmp = this.sign.compareTo(o.sign);
		if(tmp != 0){
			return tmp;
		} 
		
		tmp = this.date.compareTo(o.date);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.ip.compareTo(o.ip);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.u_sd.compareTo(o.u_sd);
		if(tmp != 0){
			return tmp;
		}
		
		return 0;
	}
	
	@Override
	public int hashCode() {
		int result = 1;
		int prime = 100;
		
		result = (result*prime) + this.sign == null ? 0 : this.sign.hashCode();
		result = (result*prime) + this.date == null ? 0 : this.date.hashCode();
		result = (result*prime) + this.ip == null ? 0 : this.ip.hashCode();
		result = (result*prime) + this.u_sd == null ? 0 : this.u_sd.hashCode();
		
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
		
		IpDimention ipDimention = (IpDimention) obj;
		
		if(this.sign == null){
			if(ipDimention.sign != null){
				return false;
			}
		}else if(ipDimention.sign == null) {
			if(this.sign != null){
				return false;
			}
		}else if(!this.sign.equals(ipDimention.sign)) {
			return false;
		}
		
		if(this.date == null){
			if(ipDimention.date != null){
				return false;
			}
		}else if(ipDimention.date == null) {
			if(this.date != null){
				return false;
			}
		}else if(!this.date.equals(ipDimention.date)) {
			return false;
		}
		
		if(this.ip == null){
			if(ipDimention.ip != null){
				return false;
			}
		}else if(ipDimention.ip == null) {
			if(this.ip != null){
				return false;
			}
		}else if(!this.ip.equals(ipDimention.ip)) {
			return false;
		}
		
		if(this.u_sd == null){
			if(ipDimention.u_sd != null){
				return false;
			}
		}else if(ipDimention.u_sd == null) {
			if(this.u_sd != null){
				return false;
			}
		}else if(!this.u_sd.equals(ipDimention.u_sd)) {
			return false;
		}
		return true;
	}
	
	@Override
	public String toString() {
		
		return this.sign + "\t" + this.date + "\t" + this.ip + this.u_sd;
	}

}
