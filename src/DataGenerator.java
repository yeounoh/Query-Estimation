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
	public Database generateDataset(String db_name, String table_name, boolean do_gen, int type, boolean pub_val_corr) throws Exception{
		Database db = new Database();
		db.connect(db_name);
		System.out.println("database connected----");
		//generate data (samples) from population
		if(do_gen){
			db.drop();
			db.connect(db_name);
			DataGenerator gen = new DataGenerator();
	
			db.createTable(table_name); 
			gen.loadDataset(db, table_name, type, pub_val_corr);
			System.out.println("database connected----");
			if(type == 3){ //GDP value cleaning
				//Data cleaning using Wikipedia GDP data
				db.createTable("wiki"); 
				gen.loadDataset(db, "wiki", 2, pub_val_corr); //syntGDP
				
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
	
	public int loadDataset(Database db, String table, int type, boolean pub_val_corr) throws SQLException, IOException, ParseException {
		String base_dir = "/Users/yeounoh/git/Query-Estimation/data/";
		
		String wikiGDP = base_dir + "gdp_us.csv";
		
		String realGDP = table == "gdp"? base_dir + "GDP2012_Run1_marked.csv": //table: gdp (single data set exp)
			table == "gdp1"? base_dir + "gdp1_marked.csv": //table: gdp1
			table == "gdp2"? base_dir + "gdp2_marked.csv": // table: gdp2
			table == "gdp3"? base_dir + "gdp3_marked.csv":null; // table: gdp3
		
		String inputEmpl = table == "employee"? base_dir + "siliconvalley1_marked.csv":
			base_dir + "siliconvalley2_marked.csv";
		//inputEmpl = table == "employee12"? base_dir + "siliconvalley12_marked.csv":inputEmpl;
		//String inputEmpl2 = base_dir + "UStech_employees1_2_marked.csv";
		
		String inputRevenue = table == "revenue"? base_dir + "siliconvalley_revenue1_marked.csv":
			base_dir + "siliconvalley_revenue2_marked.csv";
		inputRevenue = table == "revenue12"? base_dir + "siliconvalley_revenue12_marked.csv":inputRevenue;
		
		String inputEVM = base_dir + "RawResults_EVM.csv";
		
		String inputEVM_App = base_dir + "Appendicitis_EVM.csv";
		
		String inputVLDB = base_dir + "vldbsigmod_marked.csv";
		
		FileInputStream fis = null; 
		BufferedReader br = null; 
		SimpleDateFormat t = null;
		
		String line;
		int cnt = 0;
		if(type == 1 || type == 8 || type == 9 || type == 11){
			// 1~max integer values
			HashMap<Integer,String> rank_map = new HashMap<Integer,String>();
			int n_item = 100;
			int gap = 1000/n_item;
			for(int i=0;i<n_item;i++){
				int rank = n_item - i;
				if(!pub_val_corr){
					rank = (int) (n_item*Math.random() + 1); 
					while(rank_map.containsKey(rank)){
						rank = (int) (n_item*Math.random() + 1);
					}
					rank_map.put(rank,"");
				}
				DataItem s = new DataItem(new String[]{"",""},0,Integer.toString((i+1)*gap),(i+1)*gap,rank);
				db.insert(table, s);
				cnt++;
			}
		}
		if(type == 2){ //still need to clean real GDP data (type: 3)
			fis= new FileInputStream(wikiGDP);
			br= new BufferedReader(new InputStreamReader(fis));
			
			for(int i=0;i<4;i++){
				System.out.println(br.readLine());
			}
			
			while((line = br.readLine())!=null){
				String[] tokens = line.split(",");
				String[] ids = {"0","0","0"};
				long timestamp = 0;
				String name = tokens[0].substring(2); // "?California -> California
				double value = Integer.parseInt(tokens[2]);
				DataItem s = new DataItem(ids, timestamp, name, value, Integer.parseInt(tokens[1]));
				
				db.insert(table, s);
			}
			br.close();
			return cnt;
		}
		else if(type == 3 || type == 4 || type == 10){
			//real AMT (GDP, employee, revenue)
			switch(type){
				case 3: //GDP
					fis = new FileInputStream(realGDP);
					t = table == "gdp"? new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'", Locale.ENGLISH):
						new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
					break;
				case 4: //employment
					fis = new FileInputStream(inputEmpl); 
					t = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
					break;
				case 10: //revenue
					fis = new FileInputStream(inputRevenue); 
					t = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
					break;
				default: return -1;
			}
			br = new BufferedReader(new InputStreamReader(fis));
			System.out.println(br.readLine()); //skip attribute names

			HashMap<DataItem,Integer> data = new HashMap<DataItem,Integer>();
			while((line = br.readLine())!=null){ 
				String[] tokens = line.split(",");
				String[] ids = {tokens[1],tokens[0]};
				long timestamp = t.parse(tokens[3]).getTime();
				double value = Double.parseDouble(tokens[7]);  
				DataItem s = new DataItem(ids, timestamp, tokens[6], value, 0);
				data.put(s,null);
			}
			br.close();
			
			Object[] samples = data.keySet().toArray();
			new QuickSort().quickSort(samples,0,samples.length-1);
			int r = samples.length; 
			
			String n = ((DataItem) samples[0]).name(); 
			for(Object s : samples){
				if(!((DataItem) s).name().equals(n)){
					n = ((DataItem) s).name();
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
			}
			br.close();
			
			Object[] samples = data.keySet().toArray();
			new QuickSort().quickSort(samples,0,samples.length-1);
			int r = samples.length; 
			String n = ((DataItem) samples[0]).name(); 
			for(Object s : samples){
				if(!((DataItem) s).name().equals(n)){
					n = ((DataItem) s).name();
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
