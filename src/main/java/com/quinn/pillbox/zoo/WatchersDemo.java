/**   
* @Title: WatchersDemo.java 
* @Package com.quinn.pillbox.zoo 
* @Description: TODO 
* @author Quinn
* @date Apr 11, 2022 9:43:46 PM 
* @version 1.0.0   
*/
package com.quinn.pillbox.zoo;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * @ClassName: WatchersDemo
 * @Description: TODO(這裡用一句話描述這個類的作用)
 * @author Quinn
 * @date Apr 11, 2022 9:43:46 PM
 */

public class WatchersDemo implements Watcher {

	/**
	 * @Title: main
	 * @Description: TODO(這裡用一句話描述這個方法的作用)
	 * @param @param args 引數說明
	 * @return void 返回型別
	 * @throws
	 */

	private final static String ZOOKEEPER_ADDRESS = "localhost:2181";
	private final static int SESSION_TIMEOUT = 3000;
	private final static String TARGET_ZNODE = "/target_znode";
	private ZooKeeper zooKeeper;

	public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
		WatchersDemo watchersDemo = new WatchersDemo();
		watchersDemo.connectToZookeeper();
		watchersDemo.watchTargerZnode();
		watchersDemo.run();
		watchersDemo.close();
	}

	public void connectToZookeeper() throws IOException {
		this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
	}

	public void run() throws InterruptedException {
		synchronized (zooKeeper) {
			zooKeeper.wait();
		}
	}

	public void close() throws InterruptedException {
		zooKeeper.close();
	}

	public void watchTargerZnode() throws KeeperException, InterruptedException {
		Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
		if (stat == null) {
			return;
		}
		byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
		List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);
		System.out.println("Data : " + new String(data) + " children : " + children);
	}

	/*
	 * Title: process Description:
	 * 
	 * @param event
	 * 
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	@Override
	public void process(WatchedEvent event) {
		switch (event.getType()) {
		case None:
			if (event.getState() == Event.KeeperState.SyncConnected) {
				System.out.println("Successfully connected to Zookeeper");
			} else {
				synchronized (zooKeeper) {
					System.out.println("Disconnected from Zookeeper event");
					zooKeeper.notifyAll();
				}
			}
			break;
		case NodeDeleted:
			System.out.println(TARGET_ZNODE + " was deleted");
			break;
		case NodeCreated:
			System.out.println(TARGET_ZNODE + " was created");
			break;
		}
		try {
			watchTargerZnode();
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
