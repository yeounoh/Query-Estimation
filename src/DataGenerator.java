import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Random;

public class DataGenerator {
	
	/**
	 * 
	 * @param db_name
	 * @param table_name
	 * @param do_gen
	 * @param type: 1- uniform, 2- syntGDP (ground truth: 14387583), 3-realGDP, 4-solar
	 * @return
	 * @throws Exception
	 */
	public Database generateDataset(String db_name, String table_name, boolean do_gen, int type) throws Exception{
		Database db = new Database();
		db.connect(db_name);
	
		//generate data (samples) from population
		if(do_gen){
			db.drop();
			db.connect(db_name);
			DataGenerator gen = new DataGenerator();
	
			db.createTable(table_name); 
			gen.loadDataset(db, table_name, type);
			
			if(type == 3){
				//Data cleaning using Wikipedia GDP data
				db.createTable("wiki"); 
				gen.loadDataset(db, "wiki", 2); //syntGDP
				
				// assume all answers contain the precise gdp values (no variations)
				db.queryExec("create table amt_t as (select assign_id, worker_id, hit_id, "
						+ "accept_t, amt.name, wiki.value from amt left join wiki on amt.name=wiki.name)");
				db.queryExec("drop table amt");
				db.queryExec("create table amt as (select * from amt_t)");
				db.queryExec("drop table amt_t");
			}
		}
		
		return db;
	}
	
	/**
	 * type 1: uniformly distributed data items, nubmered 1 through max (e.g., 100)
	 * type 2: State,Rank,GDP(2009),Rank,GDP(2008),Rank,GDP(2007),Rank,GDP(2006),Rank,GDP(2005)
	 * type 3: AssignID | WorkerID | HITId | SubmitT | ApprovalT | State | GDP (cleaned with wiki GDP 2009 values)
	 * type 4: AssignID | WorkerID | HITId | SubmitT | ApprovalT | Company | Revenue 
	 * @param db
	 * @param table
	 * @param type
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	public int loadDataset(Database db, String table, int type) throws SQLException, IOException, ParseException {
		String syntGDP = "/Users/yeounoh/git/Query-Estimation/data/gdp_us.csv";
		String realGDP = "/Users/yeounoh/git/Query-Estimation/data/GDP2012_Run1_marked.csv";
		String inputSolar = "/Users/yeounoh/git/Query-Estimation/data/solarpanel_marked.csv";
		FileInputStream fis = null; 
		BufferedReader br = null; 
		SimpleDateFormat t = null;
		
		String line;
		int cnt = 0;
		if(type == 1){
			// 1~max integer values
			int max = 100;
			for(int i=0;i<max;i++){
				DataItem s = new DataItem(null,0,Integer.toString(i),(i+1),(max-i));
				db.insert(table, s);
				cnt++;
			}
		}
		else if(type == 2){
			//wikipedia GDP data
			fis= new FileInputStream(syntGDP);
			br= new BufferedReader(new InputStreamReader(fis));
			
			
			for(int i=0;i<4;i++){
				System.out.println(br.readLine());
			}
			
			while((line = br.readLine())!=null){
				String[] tokens = line.split(",");
				DataItem s = new DataItem(null,0,tokens[0].substring(1),Double.parseDouble(tokens[2]),Integer.parseInt(tokens[1]));
				db.insert(table, s);
				cnt++;
			}
			br.close();
		}
		else if(type == 3 || type == 4){
			//real Amazon Mechanical Turk data
			fis = type == 3 ? new FileInputStream(realGDP) : new FileInputStream(inputSolar); 
			br = new BufferedReader(new InputStreamReader(fis));
			t = type == 3 ? new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'", Locale.ENGLISH) :
				new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
			
			br.readLine(); //skip attribute names

			while((line = br.readLine())!=null){ 
				String[] tokens = line.split(",");
				String[] ids = {tokens[1],tokens[0]};
				long timestamp = t.parse(tokens[3]).getTime();
				double value = Double.parseDouble(tokens[7]); 
				DataItem s = new DataItem(ids, timestamp, tokens[6], value, 0); 
				db.insert(table, s);
				cnt++;
			}
			br.close();
		}
		
		return cnt;
	}
}
