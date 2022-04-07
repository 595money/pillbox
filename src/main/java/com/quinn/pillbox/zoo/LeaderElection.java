/**   
* @Title: LeaderElection.java 
* @Package com.quinn.pillbox.zoo 
* @Description: TODO 
* @author pigmilk
* @date Apr 8, 2022 6:18:30 AM 
* @version 1.0.0   
*/
package com.quinn.pillbox.zoo;
import org.apache.zookeeper.ZooKeeper;
/**
 * @ClassName: LeaderElection
 * @Description: TODO(這裡用一句話描述這個類的作用)
 * @author pigmilk
 * @date Apr 8, 2022 6:18:30 AM
 */
public class LeaderElection {
	
	private static final String ZOOKEEPER_ADDRESS = "http://172.17.0.4:2181";
	private ZooKeeper Zookeeper;
	public void connectToZookeeper() {

	}
	
	public static void main(String[] args) {

	}
}
