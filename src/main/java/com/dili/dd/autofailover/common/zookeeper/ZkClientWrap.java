package com.dili.dd.autofailover.common.zookeeper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.CreateMode;

/**
 * 使用自定义的ZooKeeperx for zk connection
 */
public class ZkClientWrap extends ZkClient {

	private static Map<String, ZkClientWrap> clients = new ConcurrentHashMap<String, ZkClientWrap>();

	/**
	 * 多线程同时get的时候存在双重锁定的稳定，使用请注意
	 * 
	 * @param servers
	 * @return
	 */
	public static ZkClientWrap getZkClient(String servers) {
		ZkClientWrap zk = clients.get(servers);
		if (zk == null) {
			synchronized (clients) {
				zk = new ZkClientWrap(servers);
				clients.put(servers, zk);
			}
		}
		return zk;
	}

	public static ZkClientWrap getZkClient(String servers, int sessionTimeout,
			int connectionTimeout) {
		ZkClientWrap zk = clients.get(servers);
		if (zk == null) {
			synchronized (clients) {
				zk = new ZkClientWrap(servers, sessionTimeout, connectionTimeout);
				clients.put(servers, zk);
			}
		}
		return zk;
	}

	public ZkClientWrap(String serverstring) {
		this(serverstring, Integer.MAX_VALUE);
	}

	public ZkClientWrap(String zkServers, int connectionTimeout) {
		this(new ZooKeeperClient(zkServers), connectionTimeout);
	}

	public ZkClientWrap(String zkServers, int sessionTimeout, int connectionTimeout) {
		this(new ZooKeeperClient(zkServers, sessionTimeout), connectionTimeout);
	}

	public ZkClientWrap(String zkServers, int sessionTimeout,
			int connectionTimeout, ZkSerializer zkSerializer) {
		this(new ZooKeeperClient(zkServers, sessionTimeout), connectionTimeout,
				zkSerializer);
	}

	private ZkClientWrap(IZkConnection connection, int connectionTimeout) {
		this(connection, connectionTimeout, new ByteSerializer());
	}

	private ZkClientWrap(IZkConnection zkConnection, int connectionTimeout,
			ZkSerializer zkSerializer) {
		super(zkConnection, connectionTimeout, zkSerializer);
	}

	/**
	 * Create a persistent Sequential node.
	 * 
	 * @param path
	 * @param createParents
	 *            if true all parent dirs are created as well and no
	 *            {@link ZkNodeExistsException} is thrown in case the path
	 *            already exists
	 * @throws ZkInterruptedException
	 *             if operation was interrupted, or a required reconnection got
	 *             interrupted
	 * @throws IllegalArgumentException
	 *             if called from anything except the ZooKeeper event thread
	 * @throws ZkException
	 *             if any ZooKeeper exception occurred
	 * @throws RuntimeException
	 *             if any other exception occurs
	 */
	public String createPersistentSequential(String path, boolean createParents)
			throws ZkInterruptedException, IllegalArgumentException,
			ZkException, RuntimeException {
		try {
			return create(path, null, CreateMode.PERSISTENT_SEQUENTIAL);
		} catch (ZkNoNodeException e) {
			if (!createParents) {
				throw e;
			}
			String parentDir = path.substring(0, path.lastIndexOf('/'));
			createPersistent(parentDir, createParents);
			return createPersistentSequential(path, createParents);
		}
	}

	/**
	 * Create a persistent Sequential node.
	 * 
	 * @param path
	 * @param data
	 * @param createParents
	 *            if true all parent dirs are created as well and no
	 *            {@link ZkNodeExistsException} is thrown in case the path
	 *            already exists
	 * @throws ZkInterruptedException
	 *             if operation was interrupted, or a required reconnection got
	 *             interrupted
	 * @throws IllegalArgumentException
	 *             if called from anything except the ZooKeeper event thread
	 * @throws ZkException
	 *             if any ZooKeeper exception occurred
	 * @throws RuntimeException
	 *             if any other exception occurs
	 */
	public String createPersistentSequential(String path, Object data,
			boolean createParents) throws ZkInterruptedException,
			IllegalArgumentException, ZkException, RuntimeException {
		try {
			return create(path, data, CreateMode.PERSISTENT_SEQUENTIAL);
		} catch (ZkNoNodeException e) {
			if (!createParents) {
				throw e;
			}
			String parentDir = path.substring(0, path.lastIndexOf('/'));
			createPersistent(parentDir, createParents);
			return createPersistentSequential(path, data, createParents);
		}
	}

	/**
	 * Create a persistent Sequential node.
	 * 
	 * @param path
	 * @param data
	 * @param createParents
	 *            if true all parent dirs are created as well and no
	 *            {@link ZkNodeExistsException} is thrown in case the path
	 *            already exists
	 * @throws ZkInterruptedException
	 *             if operation was interrupted, or a required reconnection got
	 *             interrupted
	 * @throws IllegalArgumentException
	 *             if called from anything except the ZooKeeper event thread
	 * @throws ZkException
	 *             if any ZooKeeper exception occurred
	 * @throws RuntimeException
	 *             if any other exception occurs
	 */
	public void createPersistent(String path, Object data, boolean createParents)
			throws ZkInterruptedException, IllegalArgumentException,
			ZkException, RuntimeException {
		try {
			create(path, data, CreateMode.PERSISTENT);
		} catch (ZkNodeExistsException e) {
			if (!createParents) {
				throw e;
			}
		} catch (ZkNoNodeException e) {
			if (!createParents) {
				throw e;
			}
			String parentDir = path.substring(0, path.lastIndexOf('/'));
			createPersistent(parentDir, createParents);
			createPersistent(path, data, createParents);
		}
	}
}
