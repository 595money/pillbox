/**   
* @Title: LeaderElection.java 
* @Package com.quinn.pillbox.zoo 
* @Description: TODO 
* @author pigmilk
* @date Apr 8, 2022 6:18:30 AM 
* @version 1.0.0   
*/
package com.quinn.pillbox.zoo;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * @ClassName: LeaderElection
 * @Description: 實作節點選舉
 * @author pigmilk
 * @date Apr 8, 2022 6:18:30 AM
 */
public class LeaderElection implements Watcher {

	private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
	private static final int SESSION_TIMEOUT = 3000;
	private static final String ELECTION_NAMESPACE = "/election";
	private ZooKeeper zooKeeper;
	private String currenZodeName;

	/**
	 * @Title: volunteerForLeadership
	 * @Description: 創建與初始化 znodes 於 tree
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @return void 返回型別
	 * @throws
	 */
	public void volunteerForLeadership() throws KeeperException, InterruptedException {
		String znodePrefix = ELECTION_NAMESPACE + "/c_";

		// 1.OPEN_ACL_UNSAFE: ACL(Access Control List), 設定可以對自己存取的白名單(IP list)
		// 2.CreateMode.EPHEMERAL_SEQUENTIAL:
		// 2.1.EPHEMERAL: 當 client 因任何形式與 znode server disconnected 時, EPHEMERAL
		// 節點將會被刪除
		// 2.1.1.透過 EPHEMERAL 失聯及刪除與 watcher 隨時監控的特性, 作到節點與領導的控制
		// 2.2.SEQUENTIAL: 節點命名方式, 單純以 order 累計
		String znodeFullPath = zooKeeper.create(znodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("znode name" + znodeFullPath);
		this.currenZodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
	}

	/**
	 * @Title: electLeader
	 * @Description: 節點選舉演算法
	 * @return void
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void electLeader() throws KeeperException, InterruptedException {
		List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

		// 1. 對 children list 做 natural ordering
		Collections.sort(children);

		// 2. 取得 "最小" 節點
		String smallestChild = children.get(0);

		// 3. 判斷自己是否為 "最小", 是則設定為 leader 節點, 若不是則依舊維持為一般節點
		if (smallestChild.equals(currenZodeName)) {
			System.out.println("I am the leader");
			return;
		}
		System.out.println("I am not the leader, " + smallestChild + "is the leader");
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
		// event trigger
		switch (event.getType()) {
		case None:
			if (event.getState() == Event.KeeperState.SyncConnected) {
				System.out.println("Sucessfully connected to Zookeeper");
			} else {
				synchronized (zooKeeper) {
					System.out.println("Disconnected from Zookeeper event");
					zooKeeper.notifyAll();
				}

			}
		}

	}

	public void connectToZookeeper() throws IOException {
		this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
	}

	public void close() throws InterruptedException {
		zooKeeper.close();
	}

	public void run() throws InterruptedException {
		synchronized (zooKeeper) {
			zooKeeper.wait();
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		LeaderElection leaderElection = new LeaderElection();
		leaderElection.connectToZookeeper();
		leaderElection.volunteerForLeadership();
		leaderElection.electLeader();
		leaderElection.run();
		leaderElection.close();
		System.out.println("Disconnected from Zookeeper, exiting application");
	}

}
