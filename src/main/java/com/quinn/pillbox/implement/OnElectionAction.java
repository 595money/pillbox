package com.quinn.pillbox.implement;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.zookeeper.KeeperException;

import com.quinn.pillbox.feature.OnElectionCallback;
import com.quinn.pillbox.zoo.management.ServiceRegistry;

/**
 * @author pigmilk
 * @date Apr 27, 2022 10:30:48 PM
 */
public class OnElectionAction implements OnElectionCallback {
	private final ServiceRegistry serviceRegistry;
	private final int port;

	public OnElectionAction(ServiceRegistry serviceRegistry, int port) {
		this.serviceRegistry = serviceRegistry;
		this.port = port;
	}

	/*
	 * Title: onElectedToBeLeader Description:
	 * 
	 * @see com.quinn.pillbox.feature.OnElectionCallback#onElectedToBeLeader()
	 */
	@Override
	public void onElectedToBeLeader() {
		try {
			serviceRegistry.unregisterFromCluster();
			serviceRegistry.registerForUpdate();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	/*
	 * Title: onWorker Description:
	 * 
	 * @see com.quinn.pillbox.feature.OnElectionCallback#onWorker()
	 */
	@Override
	public void onWorker() {
		String currentServerAddress;

		try {
			currentServerAddress = String.format("http://%s:%d", InetAddress.getLocalHost().getCanonicalHostName(),
					port);

			serviceRegistry.registerToCluster(currentServerAddress);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

}
