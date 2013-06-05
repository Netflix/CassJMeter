package com.matriks.jmeter.connections.datastaxclient;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.netflix.jmeter.connections.a6x.AstyanaxConnection;
import com.netflix.jmeter.sampler.Connection;
import com.netflix.jmeter.sampler.Operation;

public class DataStaxClientConnection extends Connection {
	private static final Logger logger = LoggerFactory.getLogger(AstyanaxConnection.class);
	private Session session;
	private Cluster cluster;
	public static final DataStaxClientConnection instance = new DataStaxClientConnection();
	public Properties config = new Properties();

	public Session session() {
		if (session != null)
			return session;

		synchronized (DataStaxClientConnection.class) {
			if (session != null)
				return session;

			try {
				File propFile = new File("cassandra.properties");
				if (propFile.exists()) {
					config.load(new FileReader(propFile));
				}
				
				cluster = Cluster.builder().addContactPoints(StringUtils.join(endpoints, ":" + port)).build();
				session = cluster.connect();
				session.execute("USE " + getKeyspaceName() + ";");
				return session;
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public Operation newOperation(String columnFamily, boolean isCounter) {
		return new DataStaxClientOperation(columnFamily, isCounter);
	}

	@Override
	public String logConnections() {
		return cluster == null ? "" : "";
	}

	@Override
	public void shutdown() {
		if (cluster != null)
			cluster.shutdown();
	}
}
