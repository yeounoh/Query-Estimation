import java.awt.List;
import java.util.ArrayList;
import java.util.Iterator;


public class Main {

	
	public static Database genCoffee(String db_name, String[] table_name, int p_size, boolean do_gen) throws Exception{
		Database db = new Database(); 
		db.connect(db_name); //if "coffee" is not present, then create a new database
		
		//generate data (samples) from population
		if(do_gen){
			db.drop();
			db.connect(db_name);
			DataGenerator gen = new DataGenerator();
			Aggregation agg = new Aggregation();
			for(int i=0;i<table_name.length;i++){
				String t_name = table_name[i];
				db.createTable(t_name); 
				gen.generateData(db, t_name, p_size, p_size*i); //male coffee consumption: Gaussian
				System.out.println("sum for "+t_name+" is "+agg.sum(db.selectAll(t_name)));
			}
		}
		
		return db;
	}
	
	public static Database genGDP(String db_name, String[] table_name, int p_size, boolean do_gen) throws Exception{
		Database db = new Database();
		db.connect(db_name);
		
		//generate data (samples) from population
		if(do_gen){
			db.drop();
			db.connect(db_name);
			DataGenerator gen = new DataGenerator();
			Aggregation agg = new Aggregation();
			for(int i=0;i<table_name.length;i++){
				String t_name = table_name[i];
				db.createTable(t_name); 
				gen.generateData(db, t_name, p_size, p_size*i); //male coffee consumption: Gaussian
				System.out.println("sum for "+t_name+" is "+agg.sum(db.selectAll(t_name)));
			}
		}
		
		return db;
	}
	
	public static void main(String[] args){
		try{
			//database configuration-1
//			String[] table_name = {"male"}; //{"male", "female"};
//			int p_size = 5000; 
//			String db_name = "coffee";
//			boolean do_gen = false; //generate new coffee data set?
//			Database db = genCoffee(db_name, table_name, p_size, do_gen);
			
			//database configuration-2
			String[] table_name = {"usa"}; 
			int p_size = 5000; 
			String db_name = "coffee";
			boolean do_gen = false; //generate new coffee data set?
			Database db = genCoffee(db_name, table_name, p_size, do_gen);
			
			//experiment configuration
			int[] s_size = {250, 500, 1000, 1500, 2000, 2500, 3000}; 
			int[] n_itr = {1, 2, 3};
			int sampling_type = 1; //1: with replacement, 2: without replacement
			int nbuckets = 5;
			int[] dist = {1, 1, 1, 1, 1, 1, 1, 1, 2, 3}; //{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}; //
			
			//estimate population statistics from samples (method1)
			for(int i=0;i<s_size.length;i++){
				for(int j=0;j<n_itr.length;j++){ 
					ArrayList<Person> samples = new ArrayList<Person>(); 
					for(int jj=0;jj<n_itr[j];jj++){//number of workers
						for(int k=0;k<table_name.length;k++){
							Person[] sample = db.sample(s_size[i]*3, p_size, table_name[k], sampling_type);
							
							//resampling: likelihood of individual to be selected
							ArrayList<Person> resampled = db.resample(s_size[i], sample, dist);
							
							//worker arrival rates?
							Iterator itr = resampled.iterator();
							while(itr.hasNext()){
								samples.add((Person) itr.next());
							}
						}
					}
					Person[] samples_arr = new Person[samples.size()];
					Object[] temp = samples.toArray();
					for(int ii=0;ii<samples_arr.length;ii++){
						samples_arr[ii]=(Person) temp[ii];
					}
					Estimator est = new Estimator(samples_arr);	
					System.out.println(" "+samples.size()+" "+est.chao92()+" "+est.sum()); //sum(*)
				}
			}
			
			//estimate population statistics from samples (method2)
			for(int i=0;i<s_size.length;i++){
				for(int j=0;j<n_itr.length;j++){
					ArrayList<Person> samples = new ArrayList<Person>(); 
					int idx= 0;
					for(int jj=0;jj<n_itr[j];jj++){
						for(int k=0;k<table_name.length;k++){
							Person[] sample = db.sample(s_size[i]*3, p_size, table_name[k], sampling_type);
							
							//resampling: likelihood of individual to be selected
							ArrayList<Person> resampled = db.resample(s_size[i], sample, dist);
							
							//worker arrival rates?
							Iterator itr = resampled.iterator();
							while(itr.hasNext()){
								samples.add((Person) itr.next());
							}
						}
					}
					
					
					int[] cnts= new int[nbuckets]; //# cups of coffee
					Person[][] buckets = new Person[nbuckets][];;
					for(Person s:samples){
						if(s==null)
							continue;
						int nc = s.getCoffee();
						cnts[nc%nbuckets]++;
					}
					for(int bi=0;bi<buckets.length;bi++){
						buckets[bi]= new Person[cnts[bi]];
					}
					
					int[] cnts2= new int[nbuckets];
					for(Person s:samples){
						if(s==null)
							continue;
						
						int nc = s.getCoffee();
						buckets[nc%nbuckets][cnts2[nc%nbuckets]++]= s;
					}
					Estimator[] ests= new Estimator[nbuckets];
					for(int ei=0;ei<ests.length;ei++){
						ests[ei] = new Estimator(buckets[ei]);
					}					
					
					System.out.print(""+samples.size());
					for(int pi=0;pi<ests.length;pi++){
						System.out.print(" "+cnts[pi]+" "+ests[pi].sum()); //count(*)	
					}
					System.out.println();
				}
			}
			
			db.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
