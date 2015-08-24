package useful;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class PreparedStatementGenerator {

	public static void main(String [] args) throws SQLException {
		// Define things for DriverManager.getConnection
		String url = "jdbc:mysql://localhost:3306/";
		//String dbName = "NBA";
		String dbName = "NBA";
		String userName = "root"; 
		String password = "";
		
		Connection con = null;
		Statement statement = null;
		ResultSet resultSet = null;
		
		StringBuffer preparedStatementStringBuffer = new StringBuffer();
		
		String query = "select * from play_by_play_text";
		ResultSetMetaData resultSetMetaData = null;
		int numberOfColumns = -1;
		
		try {
			con = DriverManager.getConnection(url+dbName,userName,password);
			con.setAutoCommit(false);	
			
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
			
			System.out.println(preparedStatementStringBuffer.toString());
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
				
	}
	
}
