package edu.isi.dig.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.dbutils.DbUtils;

public class DBHandler {
	
	String userName;
	String paswd;
	String dbUrl;
	Connection connection;
	
	public DBHandler(String userName, String paswd, String dbUrl){
		
		this.userName = userName;
		this.paswd  = paswd;
		this.dbUrl = dbUrl;
	}
	
	
	
	public String getResultsJson(String dbName, String tableName, String sha, String epoch){
		
		ResultSet rs=null;
		try {
			
			Class.forName("com.mysql.jdbc.Driver");
			
			connection = DriverManager.getConnection("jdbc:mysql://" + dbUrl + "/" + dbName + "?user=" + userName + "&password=" + paswd);
			
			String sqlQuery = "select * from " + tableName + " where sha = " + sha + " and epoch = " + epoch;
			
			Statement statement = connection.createStatement();
			
			 rs = statement.executeQuery(sqlQuery);
			 
			 if(rs.next() && rs.getInt("count") == 1){
				 return rs.getString("snapshot");
				 //return rs.getString(0);
			 }
			 else {
				 return "nothing found, keep going";
			 }
			
			
			 
		}catch(SQLException sqle){
			
			return sqle.toString();
		}catch(ClassNotFoundException cnfe){
			return cnfe.getMessage();
		}
		finally {
			
			DbUtils.closeQuietly(connection);
			DbUtils.closeQuietly(rs);
		}
		
	}
	
	/*public String getAd(String id) {
		
		ResultSet rs=null;
		try {
			
			Class.forName("com.mysql.jdbc.Driver");
			
			connection = DriverManager.getConnection("jdbc:mysql://" + dbUrl + "/" + dbName + "?user=" + userName + "&password=" + paswd);
			
			String sqlQuery = "select *,count(1) as count from ads where snapshot='" + id + "'";
			
			Statement statement = connection.createStatement();
			
			 rs = statement.executeQuery(sqlQuery);
			 
			 if(rs.next() && rs.getInt("count") == 1){
				 return rs.getString("snapshot");
				 //return rs.getString(0);
			 }
			 else {
				 return "nothing found, keep going";
			 }
			
			
			 
		}catch(SQLException sqle){
			
			return sqle.toString();
		}catch(ClassNotFoundException cnfe){
			return cnfe.getMessage();
		}
		finally {
			
			DbUtils.closeQuietly(connection);
			DbUtils.closeQuietly(rs);
		}
		
	}
	
public String getImage(String id) {
		
		ResultSet rs=null;
		try {
			
			Class.forName("com.mysql.jdbc.Driver");
			
			connection = DriverManager.getConnection("jdbc:mysql://" + dbUrl + "/" + dbName + "?user=" + userName + "&password=" + paswd);
			
			String sqlQuery = "select * from images where snapshot=" + id;
			
			Statement statement = connection.createStatement();
			
			 rs = statement.executeQuery(sqlQuery);
			
			return rs.getString(2);
			 
		}catch(SQLException sqle){
			
			return sqle.toString();
		}catch(ClassNotFoundException cnfe){
			return cnfe.getMessage();
		}
		finally {
			
			DbUtils.closeQuietly(connection);
			DbUtils.closeQuietly(rs);
		}
		
	}
*/
}
