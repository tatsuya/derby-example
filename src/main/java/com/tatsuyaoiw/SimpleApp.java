package com.tatsuyaoiw;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

public class SimpleApp {
	private String framework = "embedded";
	private String protocol = "jdbc:derby:";

	public static void main(String[] args) {
		new SimpleApp().go(args);
		System.out.println("SimpleApp finished");
	}

	private void go(String[] args) {
		parseArguments(args);

		System.out.println("SimpleApp starting in " + framework + " mode");

		Connection conn = null;

		ArrayList<Statement> statements = new ArrayList<Statement>();
		PreparedStatement psInsert;
		PreparedStatement psUpdate;
		Statement statement;
		ResultSet rs = null;

		try {
			Properties props = new Properties();
			props.put("user", "user1");
			props.put("password", "user1");

			String dbName = "derbyDB";

			conn = DriverManager.getConnection(protocol + dbName + ";create=true", props);

			statement = conn.createStatement();
			statements.add(statement);

			statement.execute("create table location(num int, addr varchar(40))");
			System.out.println("Created table location");

			psInsert = conn.prepareStatement("insert into location values (?, ?)");
			statements.add(psInsert);

			psInsert.setInt(1, 1956);
			psInsert.setString(2, "Webster St.");
			psInsert.executeUpdate();
			System.out.println("Inserted 1956 Webster");

			psInsert.setInt(1, 1910);
			psInsert.setString(2, "Union St.");
			psInsert.executeUpdate();
			System.out.println("Inserted 1910 Union");

			psUpdate = conn.prepareStatement("update location set num=?, addr=? where num=?");
			statements.add(psUpdate);

			psUpdate.setInt(1, 180);
			psUpdate.setString(2, "Grand Ave.");
			psUpdate.setInt(3, 1956);
			psUpdate.executeUpdate();
			System.out.println("Updated 1956 Webster to 180 Grand");

			psUpdate.setInt(1, 300);
			psUpdate.setString(2, "Lakeshore Ave.");
			psUpdate.setInt(3, 180);
			psUpdate.executeUpdate();
			System.out.println("Updated 180 Grand to 300 Lakeshore");

			rs = statement.executeQuery("SELECT num, addr FROM location ORDER BY num");

			int number;
			boolean failure = false;
			if (!rs.next()) {
				failure = true;
				reportFailure("No rows in ResultSet");
			}

			if ((number = rs.getInt(1)) != 300) {
				failure = true;
				reportFailure("Wrong row returned, expected num=300, got " + number);
			}

			if (!rs.next()) {
				failure = true;
				reportFailure("Too few rows");
			}

			if ((number = rs.getInt(1)) != 1910) {
				failure = true;
				reportFailure("Wrong row returned, expected num=1910, got " + number);
			}

			if (rs.next()) {
				failure = true;
				reportFailure("Too many rows");
			}

			if (!failure) {
				System.out.println("Verified the rows");
			}

			// Delete the table
			statement.execute("drop table location");
			System.out.println("Dropped table location");

			// Commit the transaction. Any changes will be persisted to the database now.
			conn.commit();
			System.out.println("Committed the transaction");

			if (framework.equals("embedded")) {
				try {
					DriverManager.getConnection("jdbc:derby:;shutdown=true");
				} catch (SQLException e) {
					if (e.getErrorCode() == 50000 && "XJ015".equals(e.getSQLState())) {
						// We got the expected exception
						System.out.println("Derby shut down normally");
					} else {
						System.err.println("Derby did not shut down normally");
						printSQLException(e);
					}
				}
			}

		} catch (SQLException e) {
			printSQLException(e);
		} finally {
			try {
				if (rs != null) {
					rs.close();
					rs = null;
				}
			} catch (SQLException e) {
				printSQLException(e);
			}

			// Statements and PreparedStatements
			int i = 0;
			while (!statements.isEmpty()) {
				Statement st = statements.remove(i);
				try {
					if (st != null) {
						st.close();
						st = null;
					}
				} catch (SQLException e) {
					printSQLException(e);
				}
			}

			try {
				if (conn != null) {
					conn.close();
					conn = null;
				}
			} catch (SQLException e) {
				printSQLException(e);
			}
		}
	}

	private void reportFailure(String message) {
		System.err.println("\nData verification failed:");
		System.err.println('\t' + message);
	}

	private static void printSQLException(SQLException e) {
		while (e != null) {
			System.err.println("\n----- SQLException -----");
			System.err.println("  SQL State:  " + e.getSQLState());
			System.err.println("  Error Code: " + e.getErrorCode());
			System.err.println("  Message:    " + e.getMessage());
			e.printStackTrace(System.err);
			e = e.getNextException();
		}
	}

	private void parseArguments(String[] args) {
		if (args.length > 0) {
			if (args[0].equalsIgnoreCase("derbyclient")) {
				framework = "derbyclient";
				protocol = "jdbc:derby://localhost:1527/";
			}
		}
	}
}
