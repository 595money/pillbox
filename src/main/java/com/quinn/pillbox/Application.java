package com.quinn.pillbox;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.quinn.pillbox.implement.OnElectionAction;
import com.quinn.pillbox.zoo.LeaderElection;
import com.quinn.pillbox.zoo.management.ServiceRegistry;

/**
 * @author pigmilk
 * @date Apr 27, 2022 10:49:05 PM
 */

public class Application implements Watcher {
	private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
	private static final int SESSION_TIMEOUT = 3000;
	private static final int DEFAULT_PORT = 8080;
	private ZooKeeper zooKeeper;

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

		// 在不同台電腦運行時可以都設定為預設 3000 port, 若是單機測試當然就要不同 port
		int currentServerPort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
		Application app = new Application();

		// 1. zookeeper client 連接 server
		ZooKeeper zooKeeper = app.connectToZookeeper();
		
		// 2. 初始化服務註冊中心 ServiceRegistry
		ServiceRegistry serviceRegistry = new ServiceRegistry(zooKeeper);
		OnElectionAction onElectionAction = new OnElectionAction(serviceRegistry, currentServerPort);
		
		// 3. 選舉演算法
		LeaderElection leaderElection = new LeaderElection(zooKeeper, onElectionAction);

		// 4. 創建znode
		leaderElection.volunteerForLeadership();

		// 5. 進行選舉(選出最小節點為leader, 將非leader節點的的watcher指向上//一節點)
		leaderElection.reElectLeader();

		// 6. 進入迴圈, 藉由事件監聽thread 來處理是否跳離迴圈
		leaderElection.run();

		// 7. 符合關閉條件的事件
		leaderElection.close();
		System.out.println("Disconnected from Zookeeper, exiting application");
	}

	public ZooKeeper connectToZookeeper() throws IOException {
		this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
		return zooKeeper;
	}

	public void run() throws InterruptedException {
		synchronized (zooKeeper) {
			zooKeeper.wait();
		}
	}

	public void close() throws InterruptedException {
		zooKeeper.close();
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
	        }
	    }
}
