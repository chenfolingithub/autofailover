package com.dili.dd.autofailover.common.zookeeper.running;

import java.io.Serializable;

import com.alibaba.fastjson.JSONObject;

/**
 * 服务端running状态信息
 */
public class RunningInfo implements Serializable {

	private static final long serialVersionUID = 92260481691855281L;

	private String address;
	private boolean active = true;

	public RunningInfo() {
	}

	public RunningInfo(String address) {
		this.address = address;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public String toString() {
		return JSONObject.toJSONString(this);
	}
}
