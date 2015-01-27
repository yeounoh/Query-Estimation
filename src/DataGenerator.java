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
	
	//Synthetic data from Wikipedia.org
	//State,Rank,GDP(2009),Rank,GDP(2008),Rank,GDP(2007),Rank,GDP(2006),Rank,GDP(2005)
	public int loadSyntheticData(Database db, String table, int type) throws SQLException, IOException {
		int cnt = 0;
		
		if(type == 1){
			//wikipedia GDP data
			String input = "/Users/yeounoh/git/Query-Estimation/data/gdp_us.csv";
			FileInputStream fis= new FileInputStream(input);
			BufferedReader br= new BufferedReader(new InputStreamReader(fis));
			
			String line;
			for(int i=0;i<4;i++){
				System.out.println(br.readLine());
			}
			
			while((line = br.readLine())!=null){
				String[] tokens = line.split(",");
				State s = new State(tokens[0].substring(1), Integer.parseInt(tokens[1]), Double.parseDouble(tokens[2]));
				db.insert(table, s);
				cnt++;
			}
			br.close();
		}
		else if(type == 2){
			// 1~max integer values
			int max = 100;
			for(int i=0;i<max;i++){
				State s = new State(Integer.toString(i),(max-i),(i+1));
				db.insert(table, s);
				cnt++;
			}
		}
		return cnt;
	}
	
	/**Crowdsourced data
	 * type 1: AssignID | WorkerID | HITId | SubmitT | ApprovalT | State | GDP (cleaned with wiki GDP 2009 values)
	 * type 2: AssignID | WorkerID | HITId | SubmitT | ApprovalT | Company | Revenue 
	 * 
	 * @param db
	 * @param table
	 * @param type
	 * @return the number of inserted data
	 * @throws SQLException
	 * @throws IOException
	 * @throws ParseException
	 */
	public int loadRealAMT(Database db, String table, int type) throws SQLException, IOException, ParseException {
		String inputGDP = "/Users/yeounoh/git/Query-Estimation/data/GDP2012_Run1_marked.csv";
		String inputSolar = "/Users/yeounoh/git/Query-Estimation/data/solarpanel_marked.csv";
		FileInputStream fis = null; 
		BufferedReader br = null; 
		SimpleDateFormat t = null;
		
		int cnt = 0;
		
		if(type == 1){
			fis = new FileInputStream(inputGDP); 
			br = new BufferedReader(new InputStreamReader(fis));
			
			br.readLine();
			t = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'", Locale.ENGLISH);
		}
		else if(type == 2){
			fis = new FileInputStream(inputSolar);
			br = new BufferedReader(new InputStreamReader(fis));
			
			br.readLine();
			t = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
		}
		
		String line;
		while((line = br.readLine())!=null){ 
			String[] tokens = line.split(",");
			if(tokens.length < 8)
				continue;
			
			String[] ids = {tokens[0], tokens[1], tokens[2]};
			
			long time = t.parse(tokens[3]).getTime();
			double value = Double.parseDouble(tokens[7]); 
			HIT h = new HIT(ids, time, tokens[6], value); 
			db.insert(table, h);
			cnt++;
		}
		br.close();
		return cnt;
	}
}
