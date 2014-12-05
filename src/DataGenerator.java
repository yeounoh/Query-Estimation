import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Random;

public class DataGenerator {
	
	//generate samples from a uniform distribution: 100 samples values ranging from 1 to 100.
	public int genUniform(Database db, String table) throws SQLException, IOException {
		int cnt = 100;
		for(int i=0;i<cnt;i++){
			State s = new State(""+i,cnt-i,i);
			db.insert(table, s);
		}
		
		return cnt;
	}
	
	//Wikipedia example
	public int loadGDPwiki(Database db, String table) throws SQLException, IOException {
		String input = "C:\\Users\\user\\workspace\\Query Estimation\\data\\gdp_us.csv";
		FileInputStream fis= new FileInputStream(input);
		BufferedReader br= new BufferedReader(new InputStreamReader(fis));
		
		String line;
		for(int i=0;i<4;i++){
			System.out.println(br.readLine());
		}
		
		int cnt = 0;
		while((line = br.readLine())!=null){
			// State(String name, int rank, int gdp)
			String[] tokens = line.split(",");
			State s = new State(tokens[0].substring(1), Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
			db.insert(table, s);
		}
		br.close();
		return cnt;
	}
	
	//Crowdsourced data
	//AssignID | WorkerID | HITId | SubmitT | ApprovalT | TimeToC | GDP
	public int loadGDPamt(Database db, String table) throws SQLException, IOException, ParseException {
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
			
			SimpleDateFormat t = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'", Locale.ENGLISH);
			long time = t.parse(tokens[3]).getTime();
			HIT h = new HIT(ids, time, tokens[7], Integer.parseInt(tokens[8]));
			db.insert(table, h);
		}
		br.close();
		return cnt;
	}
}
