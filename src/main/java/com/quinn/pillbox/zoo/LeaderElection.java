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

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * @ClassName: LeaderElection
 * @Description: TODO(這裡用一句話描述這個類的作用)
 * @author pigmilk
 * @date Apr 8, 2022 6:18:30 AM
 */
public class LeaderElection implements Watcher{

	private static final String ZOOKEEPER_ADDRESS = "172.17.0.4:2181";
	private static final int SESSION_TIMEOUT = 3000;
	private ZooKeeper zooKeeper;

	public void connectToZookeeper() throws IOException {
		this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
	}
	
	
	public static void main(String[] args) throws IOException {
		LeaderElection leaderElection = new LeaderElection();
		leaderElection.connectToZookeeper();

	}

	/*
	* Title: process
	* Description: 
	* @param event 
	* @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent) 
	*/
	@Override
	public void process(WatchedEvent event) {
		switch(event.getType()) {
		case None:
			if(event.getState() == Event.KeeperState.SyncConnected) {
				System.out.println("Sucessfully connected to Zookeeper");
			}
		}
		
	}
}
