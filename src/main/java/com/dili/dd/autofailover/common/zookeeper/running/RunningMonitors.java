package com.dili.dd.autofailover.common.zookeeper.running;

import java.util.Map;

public class RunningMonitors {

	private static RunningInfo serverData;
	private static Map runningMonitors;

	public static RunningInfo getServerData() {
		return serverData;
	}

	public static Map<String, RunningMonitor> getRunningMonitors() {
		return runningMonitors;
	}

	public static RunningMonitor getRunningMonitor(String destination) {
		return (RunningMonitor) runningMonitors.get(destination);
	}

	public static void setServerData(RunningInfo serverData) {
		RunningMonitors.serverData = serverData;
	}

	public static void setRunningMonitors(Map runningMonitors) {
		RunningMonitors.runningMonitors = runningMonitors;
	}

}
