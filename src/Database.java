import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;
import org.apache.commons.math3.distribution.GammaDistribution;

public class Database {
	private String db_name = null;
	private Connection connect = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;
	
	/**
	 * Create a mysql database, which can contain multiple tables.
	 * @param database: database name
	 * @throws Exception: SQLException, ClassNotFoundException
	 */
	public void connect(String database) throws Exception {
		try{
			this.db_name = database;
			
			//each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");
			
			//setup the connection with the DB / no password
			connect = DriverManager.getConnection("jdbc:mysql://localhost/?"
					+ "user=root&password=");
			statement = connect.createStatement();
			statement.execute("use " + database);
		}
		catch(MySQLSyntaxErrorException e){ 
			statement.execute("create database " + database);
			statement.execute("use " + database);
		}
		catch(Exception e){
			throw e;
		}
	}
	
	/**
	 * table(source_id char(30), record_id char(30), 
	 * timestamp bigint, name char(50), value double, type int).
	 * @param table: table name
	 * @throws SQLException
	 */
	public void createTable(String table) throws SQLException {
		statement.execute("create table " + table + 
				" (source_id char(30), record_id char(30)"
				+ ", timestamp bigint, name char(50), value double, rank int)");
	}
	
	public void insert(String table, DataItem r) throws SQLException{
		preparedStatement = connect
				.prepareStatement("insert into " + table + " values (?, ?, ?, ?, ?, ?)");
		preparedStatement.setString(1, r.sourceID());
		preparedStatement.setString(2, r.recordID());
		preparedStatement.setLong(3, r.timestamp()); 
		preparedStatement.setString(4, r.name());
		preparedStatement.setDouble(5, r.value());
		preparedStatement.setInt(6, r.rank());
		preparedStatement.execute(); 
		preparedStatement.clearParameters();
	}
	
	/**
	 * @param query: SQL query to execute (Data Manipulation Language)
	 * @throws SQLException 
	 */
	public void queryExec(String query) throws SQLException{
		statement = connect.createStatement();
		statement.execute(query);
	}
	
	public Object[] sampleByTime(int s_size, String table) throws SQLException {
		Object[] result = new Object[s_size];
		statement = connect.createStatement();
		
		String query = "select * from " + table + " order by timestamp limit " + s_size;
		resultSet = statement.executeQuery(query);
		
		int idx= 0;
		while(resultSet.next()){
			String source_id = resultSet.getString("source_id");
			String record_id = resultSet.getString("record_id");
			long timestamp = resultSet.getLong("timestamp");
			String name = resultSet.getString("name");
			double value = resultSet.getDouble("value");
			int rank = resultSet.getInt("rank"); 
			String[] ids = {source_id, record_id};
			result[idx++] = new DataItem(ids, timestamp, name, value, rank);
		}
		
		return result;
	}
	
	/**
	 * One time sampling from the base table, called by a single source/worker.
	 * @param s_size: sample size
	 * @param n_class: sampling with replacement need to know the number of item classes.
	 * @param table: table name
	 * @param sampling_type: 1- sampling with replacement, 2- sampling without replacement
	 * @param lambda: value-publicity correlation factor (0- no correlation)
	 * @param shape: shape parameter of gamma publicity distribution.
	 * @return samples
	 * @throws SQLException
	 */
	public Object[] sampleByRandom(int s_size, String table, int n_class, int sampling_type, 
			double lambda, double shape) throws SQLException {
		if(sampling_type == 2 && s_size > n_class){
			System.out.println("sampleByRandom(): "+n_class+" "+s_size);
			return null;
		}
		
		Random rand = new Random();
		HashMap<Integer,String> map = new HashMap<Integer,String>();
		Object[] result = new Object[s_size];
		statement = connect.createStatement();

		int idx= 0;
		while(idx < s_size){
			//sampling with replacement
			if(sampling_type == 1){
				statement.execute("create table temp (idx int)");
				for(int i=0;i<s_size;i++){
					statement.execute("insert temp select rand()*"+n_class +"+ 1");
				}
				String query = "select * from temp r left join " + table + " s on s.rank = r.idx";
				resultSet = statement.executeQuery(query);
			}
			//sampling without replacement
			if(sampling_type == 2){ 
				String query = "select * from " + table + " order by rand() limit " + n_class;
				resultSet = statement.executeQuery(query);
			}
			
			while(resultSet.next() && idx < s_size){
				String source_id = resultSet.getString("source_id");
				String record_id = resultSet.getString("record_id");
				long timestamp = resultSet.getLong("timestamp");
				String name = resultSet.getString("name");
				double value = resultSet.getDouble("value");
				int rank = resultSet.getInt("rank"); 
				String[] ids = {source_id, record_id};
				
				double pdf = 1.0;
				// Exponential distribution, 
				// scaled by 1/lambda to ensure p>>0.1 for all items rank [0.01 ... 1.00] 
				if (shape == 1.0){
					if (lambda >= 0.0)
						pdf = Math.exp(-1*(rank)*lambda/n_class); // always accept if lambda = 0
					else //lambda < 0.0, negatively correlated
						pdf = Math.exp(-1*(n_class-rank+1)*(-1*lambda)/n_class);
				}
				// Gamma distribution (shape, lambda)
				else {
					GammaDistribution gamma = new GammaDistribution(shape, lambda);
					if (lambda > 0.0)
						pdf = gamma.density(rank)*20 + 0.1; //scaling
					else
						System.err.println("Need scale>0 for Gamma dist.");
				}
				if(rand.nextDouble() <= pdf && !map.containsKey(rank)){
					result[idx++] = new DataItem(ids, timestamp, name, value, rank);
					if(sampling_type == 2){ 
						map.put(rank, null);
					}
				}
			}
			if(sampling_type == 1)
				statement.execute("drop table temp");
		}
		return result;
	}
	
	public void writeMetaData(ResultSet resultSet) throws SQLException{
		//get metadata from the database
		System.out.println("The columns in the table are: ");
		System.out.println("Table: "+resultSet.getMetaData().getTableName(1));
		for(int i=1; i<=resultSet.getMetaData().getColumnCount();i++){
			System.out.println("Column " + i + " "+ resultSet.getMetaData().getColumnName(i));
		}
	}
	
	public void writeResultSet(ResultSet resultSet) throws SQLException{
		//resultSet is initialized before the first data set
		while (resultSet.next()){
			//it is possible to get columns via name, column # which starts at 1
			int id = resultSet.getInt("id");
			int age = resultSet.getInt("age");
			int coffee = resultSet.getInt("coffee");
			
			System.out.println("id: "+id+ ", age: "+age+", coffee: "+coffee);
		}
	}
	
	public String dbName(){
		return db_name;
	}
	
	public void drop() throws SQLException{
		statement = connect.createStatement();
		statement.execute("drop database " + db_name);
	}
	
	public void close() throws SQLException{
		if(resultSet != null)
			resultSet.close();
		if(statement != null)
			statement.close();
		if(connect != null)
			connect.close();
	}
}
