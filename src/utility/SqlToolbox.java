package utility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Set;

public class SqlToolbox {

	// Use this method so that column order never gets hard-coded when defining insert prepared statements
	public static Hashtable<String, Integer> getColumnIndeces(Connection con, String database, String table){
		Hashtable<String, Integer> columnIndeces = new Hashtable<String, Integer>();
		/*
		String url = "jdbc:mysql://localhost:3306/";
		String dbName = database;
		String userName = "root"; 
		String password = "";
		Connection con = null;
		*/
		Statement statement = null;
		ResultSet resultSet = null;
		String query = "select * from " + table;
		ResultSetMetaData resultSetMetaData = null;
		int numberOfColumns = -1;
		try {
			//con = DriverManager.getConnection(url+dbName,userName,password);
			//con.setAutoCommit(false);	
			statement = con.createStatement();
			resultSet = statement.executeQuery(query);
			resultSetMetaData = resultSet.getMetaData();
			numberOfColumns = resultSetMetaData.getColumnCount();
			String columnName = null;
			for(int columnCounter = 1; columnCounter < numberOfColumns+1; columnCounter++) {
				columnName = resultSetMetaData.getColumnName(columnCounter).toLowerCase();
				columnIndeces.put(columnName, columnCounter);
			}	
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return columnIndeces;		
	}
	
	// Use this class to always get a prepared statement
	public static String getPreparedStatement(Connection con, String database, String table) {
		/*
		String url = "jdbc:mysql://localhost:3306/";
		String dbName = database;
		String userName = "root"; 
		String password = "";		
		Connection con = null;
		*/
		Statement statement = null;
		ResultSet resultSet = null;
		StringBuffer preparedStatementStringBuffer = new StringBuffer();
		String query = "select * from "+table;
		ResultSetMetaData resultSetMetaData = null;
		int numberOfColumns = -1;
		try {
			//con = DriverManager.getConnection(url+dbName,userName,password);
			//con.setAutoCommit(false);	
			statement = con.createStatement();
			resultSet = statement.executeQuery(query);
			resultSetMetaData = resultSet.getMetaData();
			numberOfColumns = resultSetMetaData.getColumnCount();
			String tableName = resultSetMetaData.getTableName(1);
			preparedStatementStringBuffer.append("insert into " + tableName +"(");
			String columnName = null;
			for(int currentColumn = 1; currentColumn < numberOfColumns; currentColumn++) {
				columnName = resultSetMetaData.getColumnName(currentColumn);
				preparedStatementStringBuffer.append(columnName + ", ");
			}
			preparedStatementStringBuffer.append(resultSetMetaData.getColumnName(numberOfColumns) + ") values(");
			for(int currentColumn = 1; currentColumn < numberOfColumns; currentColumn++) {
				preparedStatementStringBuffer.append("?, ");
			}
			preparedStatementStringBuffer.append("?)");
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return preparedStatementStringBuffer.toString();
	}
	
	// Generic statement for storing data agnostic of column order
	public static void storeData(Connection connection, String database, String table, Hashtable<String,String> data) throws Exception{
		Hashtable<String,Integer> columnIndeces = SqlToolbox.getColumnIndeces(connection,database,table);
		String query = SqlToolbox.getPreparedStatement(connection, database, table);
		PreparedStatement ps = connection.prepareStatement(query);
		Set<String> columns = data.keySet();
		for(String column : columns){	
			if(columnIndeces.containsKey(column.toLowerCase())){
				int psIndex = columnIndeces.get(column.toLowerCase());
				String value = data.get(column);
				ps.setString(psIndex, value);
			} else {
				System.out.println(column+" not found in MySQL table: "+table+"!");
				System.exit(1);
			}
		}
		ps.executeUpdate();
		ps.close();
		connection.commit();
	}
}
