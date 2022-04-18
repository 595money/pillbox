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
import org.apache.zookeeper.data.Stat;

import io.netty.util.internal.SystemPropertyUtil;

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
	private String currentZodeName;

	/**
	 * @Title: connectToZookeeper
	 * @Description: zKclient 與 zKServer 連線
	 * @param @throws IOException 引數說明
	 * @return void 返回型別
	 * @throws
	 */
	public void connectToZookeeper() throws IOException {
		this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
	}

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
		this.currentZodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
	}

	/**
	 * @Title: reElectLeader
	 * @Description: 節點選舉演算法
	 * @return void
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void reElectLeader() throws KeeperException, InterruptedException {
		String predecessorName = "";
		Stat predecessorStat = null;
		while (predecessorStat == null) {

			List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

			// 1. 對 children list 做 natural ordering
			Collections.sort(children);

			// 2. 取得 "最小" 節點
			String smallestChild = children.get(0);

			// 3. 判斷自己是否為 "最小", 是則設定為 leader 節點, 若不是則依舊維持為一般節點
			if (smallestChild.equals(currentZodeName)) {
				System.out.println("I am the leader");
				return;
			} else {
				// 當currentZode 不是 smallestChild 時, 代表 currentZode 前面一定還有 node
				System.out.println("I am not the leader");
				int predecessorIndex = Collections.binarySearch(children, currentZodeName) - 1;
				predecessorName = children.get(predecessorIndex);

				// 透過 zookeeper.exists() 將 watcher 設定為監視 predecessorNode
				predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorName, this);
			}
		}

		System.out.println("Watching zode " + predecessorName);
		System.out.println();

	}

	/**
	 * @Title: process
	 * @Description: 依照不同的狀態處理
	 * @return void
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	@SuppressWarnings("incomplete-switch")
	@Override
	//
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
			break;
		case NodeDeleted:
			try {
				reElectLeader();
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			break;
		case NodeChildrenChanged: {
			System.out.println("NodeChildrenChanged unsupported");
			break;
		}
		case NodeCreated: {
			System.out.println("NodeCreated unsupported");
			break;
		}
		case NodeDataChanged: {
			System.out.println("NodeDataChanged unsupported");
			break;
		}
		}

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
		
		//1. zookeeper client 連接 server
		leaderElection.connectToZookeeper();
		
		//2. 創建znode
		leaderElection.volunteerForLeadership();
		
		//3. 選舉(選出最小節點為leader, 將非leader節點的的watcher指向上//一節點)
		leaderElection.reElectLeader();
		
		//4. 進入迴圈, 藉由事件監聽thread 來處理是否跳離迴圈
		leaderElection.run();
		
		//5. 符合關閉條件的事件
		leaderElection.close();
		System.out.println("Disconnected from Zookeeper, exiting application");
	}

}
