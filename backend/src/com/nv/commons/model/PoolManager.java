package com.nv.commons.model;

import java.sql.Connection;
import java.sql.SQLException;

import test.ConnectionWrapper;

import com.nv.database.DBConnector;
import com.nv.database.DBNode;
import com.nv.race.model.dto.Setting;

public class PoolManager {

	private DBConnector connector = null;

	private String name = null;

	private int counter = 0;

	private int reservedNode = 0;

	public PoolManager(String name, String properties) {
		System.out.println("branch1");		
		System.out.println("trunk1");
		System.out.println("branch2");
		System.out.println("trunk2");
		System.out.println("trunk3");
		System.out.println("trunk4");
		System.out.println("trunk5");
		System.out.println("branch3");
		System.out.println("trunk6");
		System.out.println("trunk7");
		System.out.println("trunk8");
		System.out.println("branch4");
		System.out.println("branch5");
		System.out.println("trunk9");
		
		System.out.println("new branche2 aaa");
		
		System.out.println("new branche1 AAA");
		System.out.println("new branche1 BBB");
				
		System.out.println("trunk10");
		System.out.println("new branche1 CCC");
		System.out.println("trunk11");
		try {
			connector = new DBConnector(properties);
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void setReservedNode(int reservedNode) {
		this.reservedNode = reservedNode;
	}

	public int getReservedNode() {
		return this.reservedNode;
	}

	public void disableNode(int db_id) {
		DBNode node = (DBNode) connector.getNode(db_id);
		node.setEnabled(false);
		System.out.println("Disabling " + this.name + " DB Node: " + node.getNodeID());
	}

	public void enableNode(int db_id) {
		DBNode node = (DBNode) connector.getNode(db_id);
		node.setEnabled(true);
		System.out.println("Enabling " + this.name + " DB Node: " + node.getNodeID());
	}

	public Connection get() {
		return get(-1);
	}

	public Connection get(int dbIndex) {

		DBNode node = null;
		Connection conn = null;
		if (dbIndex == -1) {
			dbIndex = incrementRACCounter();
			if (reservedNode > -1 && reservedNode < connector.getNodesSize() && dbIndex == reservedNode) {
				// skip node if node is used for specialized operations
				dbIndex = incrementRACCounter(reservedNode);
			}
		}

		if (dbIndex >= connector.getNodesSize())
			dbIndex = 0;
		try {
			node = (DBNode) connector.getNode(dbIndex);
			if (node != null && node.isEnabled()) {
				// System.out.println(node.getNodeID());
				conn = node.getConnection();
			} else {
				if (connector.getNodesSize() == 0) {
					System.out.println("Exception : No " + this.name + " servers left to remove. Size 0.");
					return null;
				} else if (connector.getNodesSize() == 1) {
					node = (DBNode) connector.getNode(dbIndex);
					if (node == null || (node != null && !node.isEnabled())) {
						System.out.println("Exception : No " + this.name
							+ " servers left to remove. Invalid Node.");
						return null;
					}
					// System.out.println(node.getNodeID());
					return node.getConnection();
				}
				return get();
			}
		} catch (Exception ex) {
			System.out.println("Exception occurred while trying to get " + this.name + " connection");
			ex.printStackTrace();
			if (connector == null || connector.getNodesSize() == 0) {
				System.out.println("Exception : No " + this.name + " servers left to remove.");
				return null;
			}
			return get();
		}
		if (Setting.ENABLE_CONNECTION_DEBUG) {
			ConnectionWrapper connectionWrapper = new ConnectionWrapper(conn);
			ConnectionMonitor.getInstance().addConnection(connectionWrapper);
			return connectionWrapper;
		} else {
			return conn;
		}
	}

	private int incrementRACCounter() {
		return incrementRACCounter(-1);
	}

	private synchronized int incrementRACCounter(int RESERVED_NODE) {
		++counter;
		if (counter >= connector.getNodesSize()) {
			counter = 0;
		}
		if (RESERVED_NODE != -1 && counter == RESERVED_NODE && connector.getNodesSize() > 1) {
			++counter;
			if (counter >= connector.getNodesSize())
				counter = 0;
		}
		return counter;
	}

	public void closeDataSourcce() {
		int nodeSize = connector.getNodesSize();
		DBNode node = null;
		for (int i = 0; i < nodeSize; i++) {
			node = (DBNode) connector.getNode(i);
			try {
				node.getDataSource().close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
	}

	public int getNodesSize() {
		return this.connector.getNodesSize();
	}
}
