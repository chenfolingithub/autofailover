package com.dili.dd.autofailover.common.zookeeper.running;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.dili.dd.autofailover.InstanceManager;
import com.dili.dd.autofailover.common.zookeeper.ZkClientWrap;

public class DefaultServerRunningListener implements RunningListener {

	protected InstanceManager instancesManager;
	private String instanceKey;
	protected ZkClientWrap zkclientx;
	private String clusterPath;
	private RunningInfo runningInfo;

	public DefaultServerRunningListener(String instanceKey,
			InstanceManager instancesManager, String clusterPath,
			RunningInfo runningInfo, ZkClientWrap zkclientx) {
		this.instancesManager = instancesManager;
		this.instanceKey = instanceKey;
		this.zkclientx = zkclientx;
		this.clusterPath = clusterPath;
		this.runningInfo = runningInfo;
	}

	public void processActiveEnter() {
		instancesManager.startInstance(instanceKey);
	}

	public void processActiveExit() {
		instancesManager.stopInstance(instanceKey);
	}

	public void processStart() {
		if (zkclientx != null) {
			final String path = clusterPath + "/" + runningInfo.getAddress();
			initPath(zkclientx, path);
			zkclientx.subscribeStateChanges(new IZkStateListener() {

				public void handleStateChanged(KeeperState state)
						throws Exception {

				}

				public void handleNewSession() throws Exception {
					initPath(zkclientx, path);
				}
			});
		}
	}

	public void processStop() {
		if (zkclientx != null) {
			final String path = clusterPath + "/" + runningInfo.getAddress();
			releasePath(zkclientx, path);
		}
	}

	private void initPath(ZkClientWrap zkclientx, String path) {
		if (zkclientx != null) {
			try {
				zkclientx.createEphemeral(path);
			} catch (ZkNoNodeException e) {
				// 如果父目录不存在，则创建
				String parentDir = path.substring(0, path.lastIndexOf('/'));
				zkclientx.createPersistent(parentDir, true);
				zkclientx.createEphemeral(path);
			} catch (ZkNodeExistsException e) {
				releasePath(zkclientx, path);
				zkclientx.createEphemeral(path);
			}

		}
	}

	private void releasePath(ZkClientWrap zkclientx, String path) {
		if (zkclientx != null) {
			zkclientx.delete(path);
		}
	}

}
