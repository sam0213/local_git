package test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import com.nv.race.model.dto.Setting;
import com.nv.utils.LogUtils;

public class StatementWrapper implements java.sql.Statement {

	private Statement statement = null;
	private ConnectionWrapper conn;
	private String sql;
	
	public StatementWrapper(ConnectionWrapper conn, Statement statement) {
		this.conn = conn;
		this.statement = statement;
	}

	public String getSql() {
		return sql;
	}
	
	private void printSQL() {
		if(!Setting.ENABLE_CONNECTION_SHOW_SQL) {
			return;
		}
		LogUtils.console.info(this.sql);
	}
	//-----------------------
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return this.statement.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return this.statement.isWrapperFor(iface);
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		this.sql = sql;
		printSQL();
		return this.statement.executeQuery(sql);
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		this.sql = sql;
		printSQL();
		return this.statement.executeUpdate(sql);
	}

	@Override
	public void close() throws SQLException {
		this.statement.close();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		return this.statement.getMaxFieldSize();
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		this.statement.setMaxFieldSize(max);
	}

	@Override
	public int getMaxRows() throws SQLException {
		return this.statement.getMaxRows();
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		this.statement.setMaxRows(max);

	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		this.statement.setEscapeProcessing(enable);

	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return this.statement.getQueryTimeout();
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		this.statement.setQueryTimeout(seconds);
	}

	@Override
	public void cancel() throws SQLException {
		this.statement.cancel();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return this.statement.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		this.statement.clearWarnings();
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		this.statement.setCursorName(name);
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		this.sql = sql;
		printSQL();
		return this.statement.execute(sql);
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return this.statement.getResultSet();
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return this.statement.getUpdateCount();
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		return this.statement.getMoreResults();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		this.statement.setFetchDirection(direction);
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return this.statement.getFetchDirection();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		this.statement.setFetchSize(rows);
	}

	@Override
	public int getFetchSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return this.statement.getResultSetConcurrency();
	}

	@Override
	public int getResultSetType() throws SQLException {
		return this.statement.getResultSetType();
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		this.sql = sql;
		printSQL();
		this.statement.addBatch(sql);
	}

	@Override
	public void clearBatch() throws SQLException {
		this.statement.clearBatch();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		return this.statement.executeBatch();
	}

	@Override
	public Connection getConnection() throws SQLException {
		//return this.statement.getConnection();
		return this.conn;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		return this.statement.getMoreResults(current);
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return this.statement.getGeneratedKeys();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		this.sql = sql;
		printSQL();
		return this.statement.executeUpdate(sql, autoGeneratedKeys);
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		this.sql = sql;
		printSQL();
		return this.statement.executeUpdate(sql, columnIndexes);
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		this.sql = sql;
		printSQL();
		return this.statement.executeUpdate(sql, columnNames);
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		this.sql = sql;
		printSQL();
		return this.statement.execute(sql, autoGeneratedKeys);
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		this.sql = sql;
		printSQL();
		return this.statement.execute(sql, columnIndexes);
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		this.sql = sql;
		printSQL();
		return this.statement.execute(sql, columnNames);
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return this.statement.getResultSetHoldability();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return this.statement.isClosed();
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		this.statement.setPoolable(poolable);
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return this.statement.isPoolable();
	}

}
