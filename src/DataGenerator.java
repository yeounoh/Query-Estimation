import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Random;

import ParserMovieLens.User;


public class DataGenerator {
	
	/**
	 * 
	 * @param db
	 * @param table
	 * @param p_size
	 * @param base base for id (e.g. starts from 0 or 100?)
	 * @param dist_type 1- uniform, 2- Gaussian
	 * @throws SQLException
	 */
	public void generateData(Database db, String table, int p_size, int base) throws SQLException {
		Person[] people = generatePeople(p_size, base);
		for(int i=0;i<people.length;i++){
			db.insert(table, people[i]);
		}
	}
	
	public void loadData(Database db, String table, int p_size, int base) throws SQLException {
		FileInputStream fis= null;
		BufferedReader br= null;
		String[] tokens= null;
		FileOutputStream fos= null, fos2= null, fos3= null;
		BufferedWriter bw= null, bw2= null, bw3= null;
		String line, wline;
		
		fis= new FileInputStream(src_dir + "/u.data");
		br= new BufferedReader(new InputStreamReader(fis));
		
		//to alleviate the issue of frequent file open/close operations
		HashMap<String, User> uid_map= new HashMap<String, User>(nuser);
		
		while((line = br.readLine())!=null){
			tokens = line.split("\t|::");
			String tuid = tokens[0]; 
			String trating = "" + tokens[1] + ":" + tokens[2];
			String ttimestamp = "" + tokens[1] + ":" + tokens[3];
			
			if(uid_map.containsKey(tuid)){
				uid_map.get(tuid).addRating(trating);
				uid_map.get(tuid).addTimestamp(ttimestamp);
			}
			else{
				User tuser = new User(Integer.parseInt(tuid),trating,ttimestamp);
				uid_map.put(tuid, tuser);
			}
		}
		br.close();
	}
	
	public Person[] generatePeople(int n, int base){	
		Person[] people = new Person[n];
		Random r = new Random();
		double std = r.nextInt(2)+0.5;
		int mean = r.nextInt(3)+1;
		for(int i=0;i<n;i++){
			//coffee: normally distributed over a 0 to 9 continuous range
			int nc = Math.max(0, (int) Math.floor(r.nextGaussian()*std+mean));
			people[i] = new Person(base+(i+1), r.nextInt(30)+10, nc);
		}
		
		return people;
	}
}
