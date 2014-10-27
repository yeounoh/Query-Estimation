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
					+ "user=root&password=asdf");
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
	
	public void createCoffeeTable(String table) throws SQLException {
		try{
			statement.execute("create table " + table + " (id int, age int, coffee int)");
		}
		catch(SQLException se){
			se.printStackTrace();
		}
	}
	
	public void createGDPTable(String table) throws SQLException {
		try{
			statement.execute("create table " + table + " (name varchar(20), rank int, gdp int)");
		}
		catch(SQLException se){
			se.printStackTrace();
		}
	}
	
	public void insert(String table, Person s) throws SQLException{
		//preparedStatements can use variables and are more efficient
		preparedStatement = connect
				.prepareStatement("insert into " + table + " values (?, ?, ?)");
		preparedStatement.setInt(1, s.getID());
		preparedStatement.setInt(2, s.getAge());
		preparedStatement.setInt(3, s.getCoffee());
		preparedStatement.execute();
	}
	
	public void insert(String table, State s) throws SQLException{
		//preparedStatements can use variables and are more efficient
		preparedStatement = connect
				.prepareStatement("insert into " + table + " values (?, ?, ?)");
		preparedStatement.setString(1, s.getName());
		preparedStatement.setInt(2, s.getRank());
		preparedStatement.setInt(3, s.getGDP());
		preparedStatement.execute();
	}
	
	public Object[] sample(int s_size, int p_size, String table, int type) throws SQLException {
		Object[] result = new Object[s_size];
		statement = connect.createStatement();
		
		//sampling with replacement
		if(type == 1){
			statement.execute("create table temp (idx int)");
			for(int i=0;i<s_size;i++){
				statement.execute("insert temp select rand()*"+p_size +"+ 1");
			}
			String query = "select * from " + table + " s join temp r on s.id = r.idx";
			resultSet = statement.executeQuery(query);
		}
		//sampling with replacement
		if(type == 3){
			statement.execute("create table temp (idx int)");
			for(int i=0;i<s_size;i++){
				statement.execute("insert temp select rand()*"+p_size +"+ 1");
			}
			String query = "select * from " + table + " s join temp r on s.rank = r.idx";
			resultSet = statement.executeQuery(query);
		}		
		if(type == 2 || type == 4){
			//SELECT * FROM table ORDER BY RAND() LIMIT 10000
			String query = "select * from " + table + " order by rand() limit " + s_size;
			resultSet = statement.executeQuery(query);
		}
		
		if(type == 1 || type == 2){
			int idx= 0;
			while(resultSet.next()){
				int id = resultSet.getInt("id");
				int age = resultSet.getInt("age");
				int coffee = resultSet.getInt("coffee");
				result[idx++] = new Person(id, age, coffee);
			}
		}
		
		if(type == 3 || type == 4){
			int idx= 0;
			while(resultSet.next()){
				String name = resultSet.getString("name");
				int rank = resultSet.getInt("rank");
				int gdp = resultSet.getInt("gdp");
				result[idx++] = new State(name, rank, gdp);
			}
		}
		
		if(type == 1 || type == 3)
			statement.execute("drop table temp");
		
		return result;
	}
	
	/**
	 * likelihood for an individual to be selected
	 * @param s_size
	 * @param sample
	 * @return
	 */
	public ArrayList<Object> resample(int s_size, Object[] sample, int[] dist){
		ArrayList<Object> resampled = new ArrayList<Object>();
		HashMap<String,String> map = new HashMap<String,String>();
		//correlation: "coffee lovers are more likely to respond."
		Random r = new Random();
		
		int cnt=0;
		while(cnt < s_size){
			int idx = r.nextInt(sample.length); //choose a sample
			if(sample[idx] instanceof Person){
				Person p = (Person) sample[idx];
				if(p != null && !map.containsKey(""+idx) && r.nextInt(10)<7+dist[p.getCoffee()]){
					resampled.add(p);
					map.put(""+idx,""+0);
					cnt++;
				}
			}
			else if(sample[idx] instanceof State){
				State s = (State) sample[idx];
				if(s != null && !map.containsKey(""+idx) && (r.nextInt(50)+20)>dist[s.getRank()-1]){
					resampled.add(s);
					map.put(""+idx,""+0);
					cnt++;
				}
			}
		}
		
		return resampled;
	}
	
	public Person[] selectAll(String from) throws SQLException {
		statement = connect.createStatement();
		resultSet = statement.executeQuery("select count(*) from " + from);
		resultSet.next();
		int n = resultSet.getInt(resultSet.getMetaData().getColumnName(1));
		Person[] people = new Person[n];
		
		resultSet = statement.executeQuery("select * from " + from);
		int idx= 0;
		while(resultSet.next()){
			int id = resultSet.getInt("id");
			int age = resultSet.getInt("age");
			int coffee = resultSet.getInt("coffee");
			people[idx++] = new Person(id, age, coffee);
		}
		
		return people;
	}
	
	public Person[] union(String[] tables) throws SQLException {
		statement = connect.createStatement(); 
		resultSet = statement.executeQuery("select count(*) from " + tables[0]);
		resultSet.next();
		int n = resultSet.getInt(1);
		
		String query = "select * from "+tables[0];
		for(int i=1;i<tables.length;i++){
			query += " union all select * from " + tables[i];
			resultSet = statement.executeQuery("select count(*) from " + tables[i]);
			resultSet.next();
			n += resultSet.getInt(1);
		}
		Person[] people = new Person[n];
		
		resultSet = statement.executeQuery(query); 
		int idx = 0;
		while(resultSet.next()){
			int id = resultSet.getInt("id");
			int age = resultSet.getInt("age");
			int coffee = resultSet.getInt("coffee");
			people[idx++] = new Person(id, age, coffee);
		}
		
		return people;
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
