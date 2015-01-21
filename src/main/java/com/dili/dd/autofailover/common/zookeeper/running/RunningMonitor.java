package com.dili.dd.autofailover.common.zookeeper.running;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dili.dd.autofailover.common.MutexLock;
import com.dili.dd.autofailover.common.zookeeper.ZkClientWrap;

/**
 * 针对server的running节点控制
 * 
 */
public class RunningMonitor {

	private static final Logger logger = LoggerFactory.getLogger(RunningMonitor.class);
	private ZkClientWrap zkClient;
	private IZkDataListener dataListener;
	private MutexLock mutex = new MutexLock(false);
	private volatile boolean release = false;
	// 当前服务节点状态信息
	private RunningInfo serverData;
	// 当前实际运行的节点状态信息
	private volatile RunningInfo activeData;
	private ScheduledExecutorService delayExector = Executors
			.newScheduledThreadPool(1);
	private int delayTime = 5;
	private RunningListener listener;
	private String runningPath;
	boolean isStart = false;

	public RunningMonitor(RunningInfo serverData, String runningPath) {
		this();
		this.runningPath = runningPath;
		this.serverData = serverData;
	}

	public RunningMonitor() {
		// 创建父节点
		dataListener = new IZkDataListener() {

			public void handleDataChange(String dataPath, Object data)
					throws Exception {
				RunningInfo runningData = JSON.toJavaObject(
						(JSON) JSONObject.parse((byte[]) data),
						RunningInfo.class);
				if (!isMine(runningData.getAddress())) {
					mutex.set(false);
				}

				if (!runningData.isActive() && isMine(runningData.getAddress())) { // 说明出现了主动释放的操作，并且本机之前是active
					release = true;
					releaseRunning();
				}

				activeData = (RunningInfo) runningData;
			}

			public void handleDataDeleted(String dataPath) throws Exception {
				mutex.set(false);
				if (!release && activeData != null
						&& isMine(activeData.getAddress())) {
					// 如果上一次active的状态就是本机，则即时触发一下active抢占
					initRunning();
				} else {
					// 否则就是等待delayTime，避免因网络瞬端或者zk异常，导致出现频繁的切换操作
					delayExector.schedule(new Runnable() {

						public void run() {
							initRunning();
						}
					}, delayTime, TimeUnit.SECONDS);
				}
			}

		};

	}

	public void start() {
		this.isStart = true;
		processStart();
		if (zkClient != null) {
			zkClient.subscribeDataChanges(runningPath, dataListener);
			initRunning();
		} else {
			processActiveEnter();// 没有zk，直接启动
		}
	}

	public void release() {
		if (zkClient != null) {
			releaseRunning(); // 尝试一下release
		} else {
			processActiveExit(); // 没有zk，直接启动
		}
	}

	public void stop() {

		if (zkClient != null) {
			zkClient.unsubscribeDataChanges(runningPath, dataListener);

			releaseRunning(); // 尝试一下release
		} else {
			processActiveExit(); // 没有zk，直接启动
		}
		processStop();
	}

	private void initRunning() {
		if (!isStart()) {
			return;
		}
		byte[] bytes = JSONObject.toJSONString(serverData).getBytes();
		try {
			mutex.set(false);
			zkClient.create(runningPath, bytes, CreateMode.EPHEMERAL);
			activeData = serverData;
			processActiveEnter();// 触发一下事件
			mutex.set(true);
		} catch (ZkNodeExistsException e) {
			bytes = zkClient.readData(runningPath, true);
			if (bytes == null) {// 如果不存在节点，立即尝试一次
				initRunning();
			} else {
				activeData = JSON.toJavaObject((JSON) JSONObject.parse(bytes),
						RunningInfo.class);
			}
		} catch (ZkNoNodeException e) {
			zkClient.createPersistent(
					runningPath.substring(0, runningPath.lastIndexOf("/")),
					true); // 尝试创建父节点
			initRunning();
		}
	}

	public boolean isStart() {
		return isStart;
	}

	/**
	 * 阻塞等待自己成为active，如果自己成为active，立马返回
	 * 
	 * @throws InterruptedException
	 */
	public void waitForActive() throws InterruptedException {
		initRunning();
		mutex.get();
	}

	/**
	 * 检查当前的状态
	 */
	public boolean check() {
		try {
			byte[] bytes = zkClient.readData(runningPath);
			RunningInfo eventData = JSON.toJavaObject(
					(JSON) JSONObject.parse(bytes), RunningInfo.class);
			activeData = eventData;// 更新下为最新值
			// 检查下nid是否为自己
			boolean result = isMine(activeData.getAddress());
			if (!result) {
				logger.warn("server is running in node "
						+ activeData.getAddress() + " , but not in node "
						+ serverData.getAddress());
			}
			return result;
		} catch (ZkNoNodeException e) {
			logger.warn("server is not run any in node");
			return false;
		} catch (ZkInterruptedException e) {
			logger.warn("server check is interrupt");
			Thread.interrupted();// 清除interrupt标记
			return check();
		} catch (ZkException e) {
			logger.warn("server check is failed");
			return false;
		}
	}

	private boolean releaseRunning() {
		if (check()) {
			zkClient.delete(runningPath);
			mutex.set(false);
			processActiveExit();
			return true;
		}

		return false;
	}

	// ====================== helper method ======================

	private boolean isMine(String address) {
		return address.equals(serverData.getAddress());
	}

	private void processStart() {
		if (listener != null) {
			try {
				listener.processStart();
			} catch (Exception e) {
				logger.error("processStart failed", e);
			}
		}
	}

	private void processStop() {
		if (listener != null) {
			try {
				listener.processStop();
			} catch (Exception e) {
				logger.error("processStop failed", e);
			}
		}
	}

	private void processActiveEnter() {
		if (listener != null) {
			try {
				listener.processActiveEnter();
			} catch (Exception e) {
				logger.error("processActiveEnter failed", e);
			}
		}
	}

	private void processActiveExit() {
		if (listener != null) {
			try {
				listener.processActiveExit();
			} catch (Exception e) {
				logger.error("processActiveExit failed", e);
			}
		}
	}

	public void setListener(RunningListener listener) {
		this.listener = listener;
	}

	// ===================== setter / getter =======================

	public void setDelayTime(int delayTime) {
		this.delayTime = delayTime;
	}

	public void setServerData(RunningInfo serverData) {
		this.serverData = serverData;
	}

	public void setZkClient(ZkClientWrap zkClient) {
		this.zkClient = zkClient;
	}

}
