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
	
	public boolean isNumeric(String str)  
	{  
	  try  
	  {  
	    double d = Double.parseDouble(str);  
	  }  
	  catch(NumberFormatException nfe)  
	  {  
	    return false;  
	  }  
	  return true;  
	}
	
	/**
	 * 
	 * @param db_name
	 * @param table_name
	 * @param do_gen
	 * @param type: 1- uniform, 2- syntGDP (ground truth: 14387583), 3-realGDP, 4-employee, 5-EBM
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
				db.queryExec("create table " + table_name + "_t as (select a.source_id, a.record_id, a.timestamp, "
						+ "a.name, wiki.value, wiki.rank from " + table_name + " a left join wiki on a.name=wiki.name)");
				db.queryExec("drop table " + table_name);
				db.queryExec("create table " + table_name + " as (select * from " + table_name + "_t)");
				db.queryExec("drop table " + table_name + "_t");
			}
		}
		
		return db;
	}
	
	/**
	 * type 1: uniformly distributed data items, nubmered 1 through max (e.g., 100)
	 * type 2: State,Rank,GDP(2009),Rank,GDP(2008),Rank,GDP(2007),Rank,GDP(2006),Rank,GDP(2005)
	 * type 3: AssignID | WorkerID | HITId | AcceptT | SubmitT | ApprovalT | State | GDP (cleaned with wiki GDP 2009 values)
	 * type 4: AssignID | WorkerID | HITId | AcceptT | SubmitT | ApprovalT | Company | Revenue 
	 * type 5: AssignID | WorkerID | HITId | AcceptT | SubmitT | ApprovalT | Abs. ID | #participants
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
		String inputEmpl = "/Users/yeounoh/git/Query-Estimation/data/siliconvalley1_marked.csv";
		String inputEVM = "/Users/yeounoh/git/Query-Estimation/data/RawResults_EVM.csv";
		String inputEVM_App = "/Users/yeounoh/git/Query-Estimation/data/Appendicitis_EVM.csv";
		String inputVLDB = "/Users/yeounoh/git/Query-Estimation/data/vldbsigmod_marked.csv";
		FileInputStream fis = null; 
		BufferedReader br = null; 
		SimpleDateFormat t = null;
		
		String line;
		int cnt = 0;
		if(type == 1 || type == 8 || type == 9){
			// 1~max integer values
			int max = 100;
			for(int i=0;i<max;i++){
				boolean pub_val_corr = true; //publicity-value correlated?
				int rank = pub_val_corr ? max - i : (int) (100*Math.random() + 1); 
				DataItem s = new DataItem(new String[]{"",""},0,Integer.toString(i+1),(i+1),(max-i));
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
				DataItem s = new DataItem(new String[]{"",""},0,tokens[0].substring(1),Double.parseDouble(tokens[2]),Integer.parseInt(tokens[1]));
				db.insert(table, s);
				cnt++;
			}
			br.close();
		}
		else if(type == 3 || type == 4){
			//real Amazon Mechanical Turk data
			fis = type == 3 ? new FileInputStream(realGDP) : new FileInputStream(inputEmpl); 
			br = new BufferedReader(new InputStreamReader(fis));
			t = type == 3 ? new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'", Locale.ENGLISH) :
				new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
			//t = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
			br.readLine(); //skip attribute names

			HashMap<DataItem,Integer> data = new HashMap<DataItem,Integer>();
			while((line = br.readLine())!=null){ 
				String[] tokens = line.split(",");
				String[] ids = {tokens[1],tokens[0]};
				long timestamp = t.parse(tokens[3]).getTime();
				double value = Double.parseDouble(tokens[7]); 
				DataItem s = new DataItem(ids, timestamp, tokens[6], value, 0);
				data.put(s,null);
				//db.insert(table, s);
				//cnt++;
			}
			br.close();
			
			Object[] samples = data.keySet().toArray();
			new QuickSort().quickSort(samples,0,samples.length-1);
			int r = samples.length; double v = ((DataItem) samples[0]).value(); 
			for(Object s : samples){
				if(((DataItem) s).value() > v){
					v = ((DataItem) s).value();
					r--;
				}
				((DataItem) s).setRank(r);
				db.insert(table, (DataItem) s);
			}
		}
		else if(type == 5 || type == 6){
			//real Evidence Based Medicine data wiht Q4 (how many participants?)
			fis = type == 5 ? new FileInputStream(inputEVM) : new FileInputStream(inputEVM_App); 
			br = new BufferedReader(new InputStreamReader(fis));
			t = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'", Locale.ENGLISH);
			
			HashMap<DataItem,Integer> data = new HashMap<DataItem,Integer>();
			while((line = br.readLine())!=null){ 
				String[] tokens = line.split(",");
				String[] ids = {tokens[1],tokens[0]};
				long timestamp = t.parse(tokens[3]).getTime(); 
				double value = Double.parseDouble(tokens[7]); 
				DataItem s = new DataItem(ids, timestamp, tokens[6], value, 0); 
				data.put(s,null);
				//db.insert(table, s);
				//cnt++;
			}
			br.close();
			
			Object[] samples = data.keySet().toArray();
			new QuickSort().quickSort(samples,0,samples.length-1);
			int r = samples.length; double v = ((DataItem) samples[0]).value(); 
			for(Object s : samples){
				if(((DataItem) s).value() > v){
					v = ((DataItem) s).value();
					r--;
				}
				((DataItem) s).setRank(r);
				db.insert(table, (DataItem) s);
			}
		}
		else if(type == 7){
			//real publication data set
			fis = new FileInputStream(inputVLDB); 
			br = new BufferedReader(new InputStreamReader(fis));
			t = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
			
			br.readLine(); //skip attribute names
			
			HashMap<String,DataItem> data = new HashMap<String,DataItem>();
			while((line = br.readLine())!=null){ 
				String[] tokens = line.split(",");
				String[] ids = {tokens[1],tokens[0]};
				long timestamp = t.parse(tokens[3]).getTime(); 
				double value = Double.parseDouble(tokens[12]); //VLDB/yr 
				DataItem s = new DataItem(ids, timestamp, tokens[6], value, 0); 
				if(data.containsKey(s.name())){
					s.setRank(data.get(s.name()).rank());
				}
				else{
					s.setRank(++cnt);
				}
				data.put(s.name(),s);
				db.insert(table, s);
				
			}
			br.close();
			System.out.println("Data generated.");
		}
		
		return cnt;
	}
}
