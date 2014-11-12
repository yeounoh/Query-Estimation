import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Random;

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
	
	//Wikipedia example (obsolete)
	public int loadGDP(Database db, String table) throws SQLException, IOException {
		String input = "C:\\Users\\user\\workspace\\Query Estimation\\data\\gdp_us.csv";
		FileInputStream fis= new FileInputStream(input);
		BufferedReader br= new BufferedReader(new InputStreamReader(fis));
		
		String line;
		for(int i=0;i<4;i++){
			System.out.println(br.readLine());
		}
		
		int cnt = 0;
		while((line = br.readLine())!=null){
			//Wikipedia Examplge (obsolete)
			String[] tokens = line.split(",");
			State s = new State(tokens[0], Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
			db.insert(table, s);
		}
		br.close();
		return cnt;
	}
	
	//Crowdsourced data
	//AssignID | WorkerID | HITId | SubmitT | ApprovalT | TimeToC | GDP
	public int loadGDP2(Database db, String table) throws SQLException, IOException {
		String input = "C:\\Users\\user\\workspace\\Query Estimation\\data\\GDP2012_Run1_marked.csv";
		FileInputStream fis= new FileInputStream(input);
		BufferedReader br= new BufferedReader(new InputStreamReader(fis));
		
		String line;
		for(int i=0;i<1;i++){
			System.out.println(br.readLine());
		}
		
		int cnt = 0;
		while((line = br.readLine())!=null){
			String[] tokens = line.split(",");
			String[] ids = {tokens[0], tokens[1], tokens[2]};
			
			SimpleDateFormat t = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSZ", Locale.ENGLISH);
			long[] times = {t.parse(tokens[3]).getTime(), t.parse(tokens[4]).getTime(), t.parse(tokens[5]).getTime()};
			
			HIT h = new HIT(ids, times, tokens[6], tokens[7]);
			db.insert(table, h);
		}
		br.close();
		return cnt;
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
