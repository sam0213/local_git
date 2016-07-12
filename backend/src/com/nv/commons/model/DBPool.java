package com.nv.commons.model;

import java.sql.Connection;
import java.sql.SQLException;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class DBPool implements ServletContextListener {

	public static PoolManager racPoolManager;
	public static PoolManager nrsPoolManager;
	public static PoolManager accPoolManager;
	public static PoolManager wpPoolManager;
	public static PoolManager tsPoolManager;
	public static PoolManager vsPoolManager;
	public static PoolManager txnPoolManager;

	public static String nodeID = null;

	public void contextInitialized(ServletContextEvent sce) {
		nodeID = (String) sce.getServletContext().getInitParameter("server");
		contextInitialized(nodeID);
	}

	// for Junit Test by Alan
	public void contextInitialized(String nodeID) {
		try {
			
			
			DBPool.nodeID = nodeID;
//			racPoolManager = new PoolManager("RAC", "properties/read.properties");
//			nrsPoolManager = new PoolManager("NRS", "properties/nrs.properties");
//			accPoolManager = new PoolManager("ACC", "properties/acc_read.properties");
//			wpPoolManager = new PoolManager("WP", "properties/wp_read.properties");
//			tsPoolManager = new PoolManager("TS", "properties/ts_read.properties");
//			txnPoolManager = new PoolManager("TXN", "properties/txn_read.properties");
//			vsPoolManager = new PoolManager("VS", "properties/vs_read.properties");
			
			{
				//40
				racPoolManager = new PoolManager("RAC", "properties/40/read_11g.properties");
				accPoolManager = new PoolManager("ACC", "properties/40/acc_read.properties");
				accPoolManager.setReservedNode(-1);
				wpPoolManager = new PoolManager("WP", "properties/40/wp_read.properties");
				tsPoolManager = new PoolManager("TS", "properties/40/ts_read.properties");
				tsPoolManager.setReservedNode(-1);
				txnPoolManager = new PoolManager("TXN", "properties/40/txn_read.properties");
				txnPoolManager.setReservedNode(-1);
			}
//			{
//				//80
//				racPoolManager = new PoolManager("RAC", "properties/80/read_11g.properties");
//				accPoolManager = new PoolManager("ACC", "properties/80/acc_read.properties");
//				accPoolManager.setReservedNode(-1);
//				wpPoolManager = new PoolManager("WP", "properties/80/wp_read.properties");
//				tsPoolManager = new PoolManager("TS", "properties/80/ts_read.properties");
//				tsPoolManager.setReservedNode(-1);
//				txnPoolManager = new PoolManager("TXN", "properties/80/txn_read.properties");
//				txnPoolManager.setReservedNode(-1);
//			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}


	public static void closeAllDataSourcce() {
		racPoolManager.closeDataSourcce();
		nrsPoolManager.closeDataSourcce();
		accPoolManager.closeDataSourcce();
		wpPoolManager.closeDataSourcce();
		tsPoolManager.closeDataSourcce();
		txnPoolManager.closeDataSourcce();
		vsPoolManager.closeDataSourcce();
	}

	public static Connection getReadConnection() throws SQLException {
		return racPoolManager.get(-1);
	}

	public static Connection getDBNodeConnection(int db_id) throws SQLException {
		return racPoolManager.get(db_id);
	}

	public static Connection getNrsConnection(int db_id) throws SQLException {
		return nrsPoolManager.get(db_id);
	}

	public static Connection getConnection() throws SQLException {
		return racPoolManager.get(-1);
	}

	public static Connection getWriteConnection() throws SQLException {
		return racPoolManager.get(-1);
	}

	public static Connection getAccountsConnection() throws SQLException {
		return tsPoolManager.get();
		// return get_ts();
	}

	public static Connection getAccountsReadConnection() throws SQLException {
		return tsPoolManager.get();
		// return get_ts();
	}

	public static Connection getTsWriteConnection() throws SQLException {
		return tsPoolManager.get();
		// return get_ts();
	}

	public static Connection getTsReadConnection() throws SQLException {
		return tsPoolManager.get();
		// return get_ts();
	}

	public static Connection getPayReportWriteConnection() throws SQLException {
		return txnPoolManager.get();
		// return get_txn();
	}

	public static Connection getPayReportReadConnection() throws SQLException {
		return txnPoolManager.get();
		// return get_txn();
	}

	public static Connection getTxnWriteConnection() throws SQLException {
		return txnPoolManager.get();
		// return get_txn();
	}

	public static Connection getTxnWriteConnection(int db_node) throws SQLException {
		return txnPoolManager.get(db_node);
		// return get_txn(db_node);
	}

	public static Connection getTxnReadConnection() throws SQLException {
		return txnPoolManager.get();
		// return get_txn();
	}

	public static Connection getForecastTxnWriteConnection() throws SQLException {
		return accPoolManager.get(-1);
	}

	public static Connection getForecastTxnWriteDBNodeConnection(int dbNode) throws SQLException {
		return accPoolManager.get(dbNode);
		// return get_acc(dbNode);
	}

	public static Connection getForecastTxnReadConnection() throws SQLException {
		return accPoolManager.get(-1);
	}

	public static Connection getWpConnection() throws SQLException {
		return wpPoolManager.get(-1);
	}

	public static Connection getWpReadConnection() throws SQLException {
		return wpPoolManager.get(-1);
	}

	public static Connection getWpDBNodeConnection(int db_id) throws SQLException {
		return wpPoolManager.get(db_id);
	}

	public static Connection getVsConnection() throws SQLException {
		return vsPoolManager.get(-1);
	}

	public static Connection getVsReadConnection() throws SQLException {
		return vsPoolManager.get(-1);
	}

	public static int getAccReadConnectorSize() {
		return accPoolManager.getNodesSize();
	}

	public static int getTxnReadConnectorSize() {
		return txnPoolManager.getNodesSize();
		// return txn_read_connector.getNodesSize();
	}

	public static Connection getWpWriteConnection2() throws SQLException {
		return accPoolManager.get(-1);
	}

	public static Connection getWpReadConnection2() throws SQLException {
		return accPoolManager.get(-1);
	}

	public static Connection getWp2DBNodeConnection(int db_id) throws SQLException {
		return accPoolManager.get(db_id);
	}

	public static Connection getFcWriteConnection2() throws SQLException {
		return txnPoolManager.get();
		// return get_txn();
	}

	public static Connection getFcReadConnection2() throws SQLException {
		return txnPoolManager.get();
		// return get_txn();
	}

	public static Connection getFc2DBNodeConnection(int db_id) throws SQLException {
		return txnPoolManager.get(db_id);
		// return get_txn(db_id);
	}

	public void contextDestroyed(ServletContextEvent sce) {
	}

	public static Connection getCitiAccReadConnection() throws SQLException {
		return accPoolManager.get(-1);
	}

}

