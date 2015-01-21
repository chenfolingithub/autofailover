package com.dili.dd.autofailover;

import com.dili.dd.autofailover.common.zookeeper.ZkClientWrap;
import com.dili.dd.autofailover.common.zookeeper.running.DefaultServerRunningListener;
import com.dili.dd.autofailover.common.zookeeper.running.RunningInfo;
import com.dili.dd.autofailover.common.zookeeper.running.RunningMonitor;

public class TestRunning {

	public void start() {
		InstanceManager manager = new InstanceManager();
		RunningInfo data1 = new RunningInfo("127.0.0.1:1234");
		final TestInstance test1 = new TestInstance();
		manager.addInstance("test", test1);
		final ZkClientWrap zkclientx = ZkClientWrap.getZkClient(
				"node2:2181,node3:2181,node4:2181", 20000, 20000);
		// 初始化系统目录
		zkclientx.createPersistent("/dd/datasync/servers/test", true);
		zkclientx.createPersistent("/dd/datasync/servers/test/cluster", true);
		RunningMonitor runningMonitor1 = new RunningMonitor(data1,
				"/dd/datasync/servers/test/running");
		runningMonitor1.setZkClient(zkclientx);
		DefaultServerRunningListener listen = new DefaultServerRunningListener(
				"test", manager, "/dd/datasync/servers/test/cluster", data1,
				zkclientx);
		runningMonitor1.setListener(listen);

		runningMonitor1.start();
		new Thread(new Runnable() {

			@Override
			public void run() {
				while (true) {
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("test1=============:" + test1.isStart());
				}
			}
		}).start();
	}

	public static void main(String[] args) {
		TestRunning test = new TestRunning();
		test.start();
		synchronized (test) {
			try {
				test.wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
