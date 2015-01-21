package com.dili.dd.autofailover.common.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ZooKeeperClient implements IZkConnection {

	private static final Logger logger = LoggerFactory
			.getLogger(ZooKeeperClient.class);
	private static final int DEFAULT_SESSION_TIMEOUT = 90000;

	private ZooKeeper _zk = null;
	private Lock _zookeeperLock = new ReentrantLock();

	private final int _sessionTimeOut;
	private String _servers;

	public ZooKeeperClient(String zkServers) {
		this(zkServers, DEFAULT_SESSION_TIMEOUT);
	}

	public ZooKeeperClient(String zkServers, int sessionTimeOut) {
		_servers = zkServers;
		_sessionTimeOut = sessionTimeOut;
	}

	public void connect(Watcher watcher) {
		_zookeeperLock.lock();
		try {
			if (_zk != null) {
				throw new IllegalStateException(
						"zk client has already been started");
			}

			try {
				logger.debug("Creating new ZookKeeper instance to connect to "
						+ _servers + ".");
				_zk = new ZooKeeper(_servers, _sessionTimeOut, watcher);
			} catch (IOException e) {
				throw new ZkException("Unable to connect to " + _servers, e);
			}
		} finally {
			_zookeeperLock.unlock();
		}
	}

	public void close() throws InterruptedException {
		_zookeeperLock.lock();
		try {
			if (_zk != null) {
				logger.debug("Closing ZooKeeper connected to " + _servers);
				_zk.close();
				_zk = null;
			}
		} finally {
			_zookeeperLock.unlock();
		}
	}

	public String create(String path, byte[] data, CreateMode mode)
			throws KeeperException, InterruptedException {
		return _zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
	}

	public void delete(String path) throws InterruptedException,
			KeeperException {
		_zk.delete(path, -1);
	}

	public boolean exists(String path, boolean watch) throws KeeperException,
			InterruptedException {
		return _zk.exists(path, watch) != null;
	}

	public List<String> getChildren(final String path, final boolean watch)
			throws KeeperException, InterruptedException {
		return _zk.getChildren(path, watch);
	}

	public byte[] readData(String path, Stat stat, boolean watch)
			throws KeeperException, InterruptedException {
		return _zk.getData(path, watch, stat);
	}

	public void writeData(String path, byte[] data) throws KeeperException,
			InterruptedException {
		writeData(path, data, -1);
	}

	public void writeData(String path, byte[] data, int version)
			throws KeeperException, InterruptedException {
		_zk.setData(path, data, version);
	}

	public States getZookeeperState() {
		return _zk != null ? _zk.getState() : null;
	}

	public ZooKeeper getZookeeper() {
		return _zk;
	}

	public long getCreateTime(String path) throws KeeperException,
			InterruptedException {
		Stat stat = _zk.exists(path, false);
		if (stat != null) {
			return stat.getCtime();
		}
		return -1;
	}

	public String getServers() {
		return _servers;
	}
}
