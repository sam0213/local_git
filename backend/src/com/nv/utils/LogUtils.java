package com.nv.utils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class LogUtils {

	/**
	 * use in common undefined event case, only for test, will not in production
	 */
	public static LogConsoleUtils console = new LogConsoleUtils();
	public static class LogConsoleUtils {
		public static Logger log = Logger.getLogger("console");
		final static String FQCN = LogConsoleUtils.class.getName();
		public void info(Object message) {
			log.log(FQCN, Level.INFO, message, null);
		}
		
		/**
		 * @param message the message object to log
		 */
		public void error(Object message) {
			log.log(FQCN, Level.ERROR, message, null);
		}
		
		/**
		 * @param message the message object to log
		 * @param t the exception to log, including its stack trace.
		 */
		public void error(Object message, Throwable t) {
			log.log(FQCN, Level.ERROR, message, t);
		}
	}
	
	/**
	 * use in catch exception case
	 */
	public static Logger logErr = Logger.getLogger("err");

	private LogUtils() {
		throw new AssertionError();
	}
}
