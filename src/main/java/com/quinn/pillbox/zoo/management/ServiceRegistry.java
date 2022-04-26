package com.quinn.pillbox.zoo.management;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import lombok.Getter;
import lombok.Setter;


/** 
* @author Quinn
* @date Apr 27, 2022 6:51:42 AM 
*/
@Getter
@Setter
public class ServiceRegistry implements Watcher {
	private static final String REGISTRY_ZNODE = "/service_registry";
	private final ZooKeeper zooKeeper;
	private String currentZnode = null;
	private List<String> allServiceAddresses;

	public ServiceRegistry(ZooKeeper zooKeeper) {
		this.zooKeeper = zooKeeper;

		// 避免多執行緒環境重複呼叫 create, 所以設計為 private method, 並於建構子中呼叫
		createServiceRegistryZnode();
	}

	public void registerToCluster(String metadata) {
		try {
			this.currentZnode = zooKeeper.create(REGISTRY_ZNODE + "/n_", metadata.getBytes(),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

			System.out.println("Registered to service registry");
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void createServiceRegistryZnode() {
		try {
			if (zooKeeper.exists(REGISTRY_ZNODE, false) == null) {
				zooKeeper.create(REGISTRY_ZNODE, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	/** 
	* 依據服務註冊中心 (service registry) 中節點的加入或移除來更新
	* @throws KeeperException
	* @throws InterruptedException
	*/
	public synchronized void updateAddresses() throws KeeperException, InterruptedException {
		List<String> workerZnodes = zooKeeper.getChildren(REGISTRY_ZNODE, this);
		List<String> addresses = new ArrayList<>(workerZnodes.size());
		for (String workerZnode : workerZnodes) {
			String workerZnodeFullPath = REGISTRY_ZNODE + "/" + workerZnode;
			Stat stat = zooKeeper.exists(workerZnodeFullPath, false);
			if (stat == null) {
				continue;
			}
			byte[] addressBytes = zooKeeper.getData(workerZnodeFullPath, false, stat);
			String address = new String(addressBytes);
			addresses.add(address);
		}
		this.allServiceAddresses = Collections.unmodifiableList(addresses);
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
		// TODO Auto-generated method stub

	}
}
