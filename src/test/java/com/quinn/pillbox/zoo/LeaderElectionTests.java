package com.quinn.pillbox.zoo;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.quinn.pillbox.feature.OnElectionCallback;

@SpringBootTest

class LeaderElectionTests {
	private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
	private static final int SESSION_TIMEOUT = 3000;
	private static final String ELECTION_NAMESPACE = "/election";
	private ZooKeeper zooKeeper;
	private String currentZodeName;

}
