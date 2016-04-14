package com.yuhe.statics.oss_statics.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DBManager {
	private static final Log log = LogFactory.getLog(DBManager.class);
	private static final String configFile = "dbcp.properties";

	private static DataSource dataSource;

	static {
		Properties dbProperties = new Properties();
		try {
			dbProperties.load(DBManager.class.getClassLoader().getResourceAsStream(configFile));
			dataSource = BasicDataSourceFactory.createDataSource(dbProperties);

			Connection conn = getConn();
			DatabaseMetaData mdm = conn.getMetaData();
			log.info("Connected to " + mdm.getDatabaseProductName() + " " + mdm.getDatabaseProductVersion());
			// System.out.println("Connected to " + mdm.getDatabaseProductName()
			// + " " + mdm.getDatabaseProductVersion());
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			log.error("初始化连接池失败：" + e);
		}
	}

	private DBManager() {
	}

	/**
	 * 获取链接，用完后记得关闭
	 *
	 * @see {@link DBManager#closeConn(Connection)}
	 * @return
	 */
	public static final Connection getConn() {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
		} catch (SQLException e) {
			log.error("获取数据库连接失败：" + e);
		}
		return conn;
	}

	/**
	 * 关闭连接
	 *
	 * @param conn
	 *            需要关闭的连接
	 */
	public static void closeConn(Connection conn) {
		try {
			if (conn != null && !conn.isClosed()) {
				conn.setAutoCommit(true);
				conn.close();
			}
		} catch (SQLException e) {
			log.error("关闭数据库连接失败：" + e);
		}
	}

	/**
	 * 查询操作，也可以做插入操作
	 * 
	 * @param sql
	 * @return
	 */
	public static ResultSet query(Connection conn, String sql) {
		ResultSet rs = null;
		try {
			Statement smst = conn.createStatement();
			rs = smst.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return rs;
	}

	public static boolean execute(String sql) {
		Connection conn = getConn();
		boolean flag = false;
		try {
			Statement smst = conn.createStatement();
			smst.executeUpdate(sql);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			closeConn(conn);
		}
		return flag;
	}
}
