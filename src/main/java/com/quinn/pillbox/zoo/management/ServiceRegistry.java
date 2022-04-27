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
 * 服務註冊中心相關處理的類別, 創建服務中心、管理服務中心相關事務
 * 
 * @author Quinn
 * @date Apr 27, 2022 6:51:42 AM
 */

@Getter
@Setter
public class ServiceRegistry implements Watcher {
	private static final String REGISTRY_ZNODE = "/service_registry";
	private final ZooKeeper zooKeeper;
	private String currentZnode = null;
	/**
	 * 作為緩存用,儲存 cluster 中所有節點 address
	 */
	private List<String> allServiceAddresses;

	public ServiceRegistry(ZooKeeper zooKeeper) {
		this.zooKeeper = zooKeeper;

		// 避免多執行緒環境重複呼叫 create, 所以設計為 private method, 並於建構子中呼叫
		createServiceRegistryZnode();
	}

	/**
	 * 由於 zookeeper 整個框架是透過記憶體實踐高效能,<br>
	 * 所以註冊中心儲存的節點共享資料 metadata 不可以太大量
	 * 
	 * @param metadata
	 */
	public void registerToCluster(String metadata) {
		try {
			// 節點名稱為 /n_HOSTNAME, 透過 metadata 取得 address
			this.currentZnode = zooKeeper.create(REGISTRY_ZNODE + "/n_", metadata.getBytes(),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

			System.out.println("Registered to service registry");
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 服務註冊中心初始化後第一次 update 使用
	 * 
	 * @param 引數說明
	 * @throws
	 */
	public void registerForUpdate() {
		try {
			updateAddresses();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public synchronized List<String> getAllServiceAddresses() throws KeeperException, InterruptedException {
		if (allServiceAddresses == null) {
			updateAddresses();
		}
		return allServiceAddresses;
	}

	/**
	 * 服務註冊中心初始化時呼叫此方法來創建 ServiceRegistryZnode
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
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
	 * 
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
		System.out.println("The cluster addresses are: " + this.allServiceAddresses);
	}

	/**
	 * 服務註冊中心移除節點的 address, <br>
	 * 當節點脫離 cluster 或節點被選為領導時, <br>
	 * 需要將此節點從 allServiceAddresses 中移除
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void unregisterFromCluster() throws KeeperException, InterruptedException {
		if (currentZnode != null && zooKeeper.exists(currentZnode, false) != null) {
			zooKeeper.delete(currentZnode, -1);
		}
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
		try {
			updateAddresses();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
