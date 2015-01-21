package com.dili.dd.autofailover;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InstanceManager {

	public static Map<String, Instance> instances = new ConcurrentHashMap<String, Instance>();

	public Map<String, Instance> getInstances() {
		return instances;
	}

	public void startInstance(String key) {
		instances.get(key).start();
	}

	public void stopInstance(String key) {
		instances.get(key).stop();
	}

	public void deleteInstance(String key) {
		instances.remove(key);
	}

	public void addInstance(String key, Instance instance) {
		this.instances.put(key, instance);
	}
}
