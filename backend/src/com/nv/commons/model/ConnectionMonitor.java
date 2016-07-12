package com.nv.commons.model;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import test.ConnectionWrapper;

import com.nv.utils.LogUtils;

public class ConnectionMonitor {

	private static final ConnectionMonitor instance = new ConnectionMonitor();
	private ConcurrentHashMap<Long, ConnectionWrapper> connestions = new ConcurrentHashMap<Long, ConnectionWrapper>();
	private AtomicLong atomic = new AtomicLong(0);

	private ConnectionMonitor() {
		super();
		try {
			Thread t = new MonitorThread();
			t.setDaemon(true);
			t.start();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public final static ConnectionMonitor getInstance() {

		return instance;
	}

	public void addConnection(ConnectionWrapper conn) {
		connestions.put(conn.getSeq(), conn);
	}

	public void removeConnection(ConnectionWrapper conn) {
		connestions.remove(conn.getSeq());
	}

	public long getAtomicInteger() {
		return atomic.incrementAndGet();
	}

	public void checkConnection() {
		long ts = System.currentTimeMillis() - 10000;

		for (ConnectionWrapper conn : connestions.values()) {
			if (ts > conn.getTimeStamp()) {
				LogUtils.console.error("connection not close over "
					+ (System.currentTimeMillis() - conn.getTimeStamp()) + " ms : " + conn.getSql() + "\n" + conn.getStackTrace());
			}
		}
	}
}

class MonitorThread extends Thread {

	public MonitorThread() {
	}

	public void run() {

		while (true) {
			try {
				Thread.currentThread().sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				ConnectionMonitor.getInstance().checkConnection();
			} catch (Exception ex1) {
				ex1.printStackTrace(System.out);
			} finally {

			}

		}
	}

}