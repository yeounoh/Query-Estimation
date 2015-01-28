import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;

public class Database {
	private String db_name = null;
	private Connection connect = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;
	
	public void connect(String database) throws Exception {
		try{
			this.db_name = database;
			
			//each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");
			
			//setup the connection with the DB
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
	
	public void createSyntTable(String table) throws SQLException {
		statement.execute("create table " + table + " (name char(50), rank int, value double not null)");	
	}
	
	public void createHITTable(String table) throws SQLException {
		statement.execute("create table " + table + 
				" (assign_id char(30), worker_id char(30), hit_id char(30)"
				+ ", accept_t bigint, name char(50), value double)");
	}
	
	public void insert(String table, State s) throws SQLException{
		//preparedStatements can use variables and are more efficient
		preparedStatement = connect
				.prepareStatement("insert into " + table + " values (?, ?, ?)");
		preparedStatement.setString(1, s.getName());
		preparedStatement.setInt(2, s.getRank());
		preparedStatement.setDouble(3, s.getGDP());
		preparedStatement.execute();
		preparedStatement.clearParameters();
	}
	
	public void insert(String table, HIT h) throws SQLException{
		preparedStatement = connect
				.prepareStatement("insert into " + table + " values (?, ?, ?, ?, ?, ?)");
		preparedStatement.setString(1, h.getAssignID());
		preparedStatement.setString(2, h.getWorkerID());
		preparedStatement.setString(3, h.getHITId());
		preparedStatement.setLong(4, h.getAcceptTime());
		preparedStatement.setString(5, h.getName());
		preparedStatement.setDouble(6, h.getValue());
		preparedStatement.execute(); 
		preparedStatement.clearParameters();
	}
	
	/**
	 * hard-coded query execution
	 * @param query
	 * @throws SQLException 
	 */
	public void queryExec(String query) throws SQLException{
		statement = connect.createStatement();
		statement.execute(query);
	}
	
	public Object[] sampleAMT(int s_size, String table) throws SQLException {
		Object[] result = new Object[s_size];
		statement = connect.createStatement();
		
		String query = "select * from " + table + " order by accept_t limit " + s_size;
		resultSet = statement.executeQuery(query);
		
		int idx= 0;
		while(resultSet.next()){
			String assign_id = resultSet.getString("assign_id");
			String worker_id = resultSet.getString("worker_id");
			String hit_id = resultSet.getString("hit_id");
			long accept_t = resultSet.getLong("accept_t");
			String name = resultSet.getString("name");
			double value = resultSet.getDouble("value"); 
			//Double d = resultSet.getObject(6) != null ? 1.0 : null; System.out.println(d);
			String[] ids = {assign_id, worker_id, hit_id};
			result[idx++] = new HIT(ids, accept_t, name, value);
		}
		
		return result;
	}
	/**
	 * this is for synthetic data set.
	 * @param s_size
	 * @param p_size
	 * @param table
	 * @param sampling_type 1- with replacement, 2- without replacment
	 * @return
	 * @throws SQLException
	 */
	public Object[] sample(int s_size, int n_class, String table, int sampling_type) throws SQLException {
		Object[] result = new Object[s_size];
		statement = connect.createStatement();
		
		//sampling with replacement
		if(sampling_type == 1){
			statement.execute("create table temp (idx int)");
			for(int i=0;i<s_size;i++){
				statement.execute("insert temp select rand()*"+n_class +"+ 1");
			}
			String query = "select * from " + table + " s join temp r on s.rank = r.idx";
			resultSet = statement.executeQuery(query);
		}	
		if(sampling_type == 2){
			//SELECT * FROM table ORDER BY RAND() LIMIT 10000
			String query = "select * from " + table + " order by rand() limit " + s_size;
			resultSet = statement.executeQuery(query);
		}
		
		int idx= 0;
		while(resultSet.next()){
			String name = resultSet.getString("name");
			int rank = resultSet.getInt("rank");
			int value = resultSet.getInt("value");
			result[idx++] = new State(name, rank, value);
		}
		
		if(sampling_type == 1){
			statement.execute("drop table temp");
		}
		
		return result;
	}
	
	/**
	 * likelihood for an individual to be selected
	 * *this is for synthetic dataset
	 * @param s_size
	 * @param sample
	 * @param lamda 
	 * @return
	 */
	public ArrayList<Object> resample(int s_size, Object[] sample, double lamda, int n_class){
		ArrayList<Object> resampled = new ArrayList<Object>();
		HashMap<String,String> map = new HashMap<String,String>();
		//correlation: "coffee lovers are more likely to respond."
		Random r = new Random();
		
		int cnt=0;
		while(cnt < s_size){
			int idx = r.nextInt(sample.length); //choose a sample
			if(sample[idx] instanceof State){
				//lamb * Math.exp(-1*lamb*)
				State s = (State) sample[idx]; 
				if(s != null && !map.containsKey(""+idx) 
						&& (r.nextDouble() < (Math.exp(-1*(s.getRank()-1)*lamda/n_class)))){
					resampled.add(s);
					map.put(""+idx,""+0);
					cnt++;
				}
			}
		}
		
		return resampled;
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
	
	public String getName(){
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
