package com.oracle.project.dimention.browser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class BrowserDimention implements WritableComparable<BrowserDimention>{
	
	private String date;
	private String sign;
	private String b_iev;
	private String u_uid;
	private String u_mid;
	private String u_sd;
	
	public String getU_sd() {
		return u_sd;
	}

	public void setU_sd(String u_sd) {
		this.u_sd = u_sd;
	}

	public String getU_mid() {
		return u_mid;
	}

	public void setU_mid(String u_mid) {
		this.u_mid = u_mid;
	}

	public String getU_uid() {
		return u_uid;
	}

	public void setU_uid(String u_uid) {
		this.u_uid = u_uid;
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

	public String getB_iev() {
		return b_iev;
	}

	public void setB_iev(String b_iev) {
		this.b_iev = b_iev;
	}
	
	public BrowserDimention() {
		
	}

	public BrowserDimention(String date, String sign, String b_iev,String u_uid,String u_mid,String u_sd) {
		this.date = date;
		this.sign = sign;
		this.b_iev = b_iev;
		this.u_uid = u_uid;
		this.u_mid = u_mid;
		this.u_sd = u_sd;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(date);
		out.writeUTF(sign);
		out.writeUTF(b_iev);
		out.writeUTF(u_uid);
		out.writeUTF(u_mid);
		out.writeUTF(u_sd);
	}

	public void readFields(DataInput in) throws IOException {
		this.date = in.readUTF();
		this.sign = in.readUTF();
		this.b_iev = in.readUTF();
		this.u_uid = in.readUTF();
		this.u_mid = in.readUTF();
		this.u_sd = in.readUTF();
	}
	
	public int compareTo(BrowserDimention o) {
		
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
		
		tmp = this.b_iev.compareTo(o.b_iev);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.u_uid.compareTo(o.u_uid);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.u_mid.compareTo(o.u_mid);
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
		
		result = (result*prime) + this.date == null ? 0 : this.date.hashCode();
		result = (result*prime) + this.sign == null ? 0 : this.sign.hashCode();
		result = (result*prime) + this.b_iev == null ? 0 : this.b_iev.hashCode();
		result = (result*prime) + this.u_uid == null ? 0 : this.u_uid.hashCode();
		result = (result*prime) + this.u_mid == null ? 0 : this.u_mid.hashCode();
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
		
		BrowserDimention browserDimention = (BrowserDimention) obj;
		
		if(this.date == null){
			if(browserDimention.date != null){
				return false;
			}
		}else if(browserDimention.date == null) {
			if(this.date != null){
				return false;
			}
		}else if(!this.date.equals(browserDimention.date)) {
			return false;
		}
		
		if(this.sign == null){
			if(browserDimention.sign != null){
				return false;
			}
		}else if(browserDimention.sign == null) {
			if(this.sign != null){
				return false;
			}
		}else if(!this.sign.equals(browserDimention.sign)) {
			return false;
		}
		
		if(this.b_iev == null){
			if(browserDimention.b_iev != null){
				return false;
			}
		}else if(browserDimention.b_iev == null) {
			if(this.b_iev != null){
				return false;
			}
		}else if(!this.b_iev.equals(browserDimention.b_iev)) {
			return false;
		}
		
		if(this.u_uid == null){
			if(browserDimention.u_uid != null){
				return false;
			}
		}else if(browserDimention.u_uid == null) {
			if(this.u_uid != null){
				return false;
			}
		}else if(!this.u_uid.equals(browserDimention.u_uid)) {
			return false;
		}
		
		if(this.u_mid == null){
			if(browserDimention.u_mid != null){
				return false;
			}
		}else if(browserDimention.u_mid == null) {
			if(this.u_mid != null){
				return false;
			}
		}else if(!this.u_mid.equals(browserDimention.u_mid)) {
			return false;
		}
		
		if(this.u_sd == null){
			if(browserDimention.u_sd != null){
				return false;
			}
		}else if(browserDimention.u_sd == null) {
			if(this.u_sd != null){
				return false;
			}
		}else if(!this.u_sd.equals(browserDimention.u_sd)) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.date + "\t" + this.sign + "\t" + this.b_iev + "\t" + this.u_uid + "\t" + u_mid + "\t" + u_sd;
	}

}
