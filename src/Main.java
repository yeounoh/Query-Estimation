import java.awt.List;
import java.util.ArrayList;
import java.util.Iterator;

// https://github.com/yeounoh/Query-Estimation.git
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
				db.createCoffeeTable(t_name); 
				gen.generateData(db, t_name, p_size, p_size*i); //male coffee consumption: Gaussian
				System.out.println("sum for "+t_name+" is "+agg.sum(db.selectAll(t_name))); //ground truth
			}
		}
		
		return db;
	}
	
	public static Database genGDP(String db_name, String[] table_name, boolean do_gen) throws Exception{
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
				db.createGDPTable(t_name); 
				gen.loadGDP(db, t_name);
				System.out.println("sum for "+t_name+" is 14387583"); //ground truth
			}
		}
		
		return db;
	}
	
	public static void main(String[] args){
		try{
			boolean coffee_eg = false;
			boolean gdp_eg = false;
			boolean gdp_real = false;
			
			if(coffee_eg){
				//database configuration-1
				String[] table_name = {"male"}; //{"male", "female"};
				int p_size = 5000; 
				String db_name = "coffee";
				boolean do_gen = false; //generate new coffee data set?
				Database db = genCoffee(db_name, table_name, p_size, do_gen);
				
				//experiment configuration
				int[] s_size = {250, 500, 1000, 1500, 2000, 2500, 3000}; 
				int[] n_itr = {1, 2, 3};
				int sampling_type = 2; //1: with replacement, 2: without replacement
				int nbuckets = 5;
				int[] dist = {1, 1, 1, 1, 1, 1, 1, 1, 2, 3}; //{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}; //
				
				//estimate population statistics from samples (method1)
				for(int i=0;i<s_size.length;i++){
					for(int j=0;j<n_itr.length;j++){ 
						ArrayList<Object> samples = new ArrayList<Object>(); 
						for(int jj=0;jj<n_itr[j];jj++){//number of workers
							for(int k=0;k<table_name.length;k++){
								Object[] sample = db.sample(s_size[i]*3, p_size, table_name[k], sampling_type);
								
								//resampling: likelihood of individual to be selected
								ArrayList<Object> resampled = db.resample(s_size[i], sample, dist);
								
								//worker arrival rates?
								Iterator itr = resampled.iterator();
								while(itr.hasNext()){
									samples.add((Person) itr.next());
								}
							}
						}
						Object[] samples_arr = new Object[samples.size()];
						Object[] temp = samples.toArray();
						for(int ii=0;ii<samples_arr.length;ii++){
							samples_arr[ii] = temp[ii];
						}
						Estimator est = new Estimator(samples_arr);	
						System.out.println(" "+samples.size()+" "+est.chao92()+" "+est.sum()); //sum(*)
					}
				}
				
				//estimate population statistics from samples (method2)
				for(int i=0;i<s_size.length;i++){
					for(int j=0;j<n_itr.length;j++){
						ArrayList<Object> samples = new ArrayList<Object>(); 
						int idx= 0;
						for(int jj=0;jj<n_itr[j];jj++){
							for(int k=0;k<table_name.length;k++){
								Object[] sample = db.sample(s_size[i]*3, p_size, table_name[k], sampling_type);
								
								//resampling: likelihood of individual to be selected
								ArrayList<Object> resampled = db.resample(s_size[i], sample, dist);
								
								//worker arrival rates?
								Iterator itr = resampled.iterator();
								while(itr.hasNext()){
									samples.add(itr.next());
								}
							}
						}
						
						int[] cnts= new int[nbuckets]; //# cups of coffee
						Object[][] buckets = new Object[nbuckets][];;
						for(Object s:samples){
							if(s==null)
								continue;
							if(s instanceof Person){
								int nc = ((Person) s).getCoffee();
								cnts[nc%nbuckets]++;
							}
							else if(s instanceof State){
								int r = ((State) s).getRank();
								cnts[r%nbuckets]++;
							}
						}
						for(int bi=0;bi<buckets.length;bi++){
							buckets[bi]= new Object[cnts[bi]];
						}
						
						int[] cnts2= new int[nbuckets];
						for(Object s:samples){
							if(s==null)
								continue;
							if(s instanceof Person){
								int nc = ((Person) s).getCoffee();
								buckets[nc%nbuckets][cnts2[nc%nbuckets]++]= s;
							}
							else if(s instanceof State){
								int r = ((State) s).getRank();
								buckets[r%nbuckets][cnts2[r%nbuckets]++]= s;
							}
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
			
			if(gdp_eg){
				//database configuration-2
				String db_name = "gdp";
				String[] table_name = {"usa", "usa_real"}; 
				int p_size = 50; 
				
				boolean do_gen = false; //generate new coffee data set?
				Database db = genGDP(db_name, table_name, do_gen);
				
				//experiment configuration
				int[] s_size = {5, 10, 15, 20, 25, 30, 35}; 
				int[] n_itr = {3, 10, 30}; // number of workers
				int sampling_type = 4; //3: with replacement, 4: without replacement
				int[] nbuckets = {1, 3, 5, 10, 20};
				double[] lamda = {0.5, 1.0}; //correlation factor
				
				//estimate population statistics from samples (method1: naive)
				for(int i=0;i<s_size.length;i++){
					for(int cri=0;cri<lamda.length;cri++){
						System.out.println("lamda: " + lamda[cri]);
						for(int j=0;j<n_itr.length;j++){
							System.out.println("#worker: "+n_itr[j]);
							
							ArrayList<Object> samples = new ArrayList<Object>(); 
							for(int jj=0;jj<n_itr[j];jj++){
								for(int k=0;k<table_name.length;k++){
									Object[] sample = db.sample(s_size[i]*3, p_size, table_name[k], sampling_type);
								
									//resampling: likelihood of individual to be selected
									//ArrayList<Object> resampled = db.resample(s_size[i], sample, dist);
									ArrayList<Object> resampled = db.resample(s_size[i], sample, lamda[cri]);
									
									//worker arrival rates?
									Iterator itr = resampled.iterator();
									while(itr.hasNext()){
										samples.add(itr.next());
									}
								}
							}
							Object[] samples_arr = new Object[samples.size()];
							Object[] temp = samples.toArray();
							for(int ii=0;ii<samples_arr.length;ii++){
								samples_arr[ii] = temp[ii];
							}
							Estimator est = new Estimator(samples_arr);	
							System.out.println(" "+samples.size()+" "+est.chao92()+" "+est.sum()); //sum(*)
						}
					}
				}
				
				//estimate population statistics from samples (method2: bucket)
				for(int i=0;i<s_size.length;i++){
					for(int cri=0;cri<lamda.length;cri++){
						System.out.println("lamda: " + lamda[cri]);
						for(int j=0;j<n_itr.length;j++){
							System.out.println("#worker: "+n_itr[j]);
							
							ArrayList<Object> samples = new ArrayList<Object>(); 
							for(int jj=0;jj<n_itr[j];jj++){
								for(int k=0;k<table_name.length;k++){
									Object[] sample = db.sample(s_size[i]*3, p_size, table_name[k], sampling_type);
									
									//resampling: likelihood of individual to be selected
									//ArrayList<Object> resampled = db.resample(s_size[i], sample, dist);
									ArrayList<Object> resampled = db.resample(s_size[i], sample, lamda[cri]);
									
									//worker arrival rates?
									Iterator itr = resampled.iterator();
									while(itr.hasNext()){
										samples.add(itr.next());
									}
								}
							}
							for(int nbi=0;nbi<nbuckets.length;nbi++){
								int[] cnts= new int[nbuckets[nbi]]; 
								Object[][] buckets = new Object[nbuckets[nbi]][];;
								for(Object s:samples){
									if(s==null)
										continue;
									if(s instanceof Person){
										int nc = ((Person) s).getCoffee();
										cnts[nc%nbuckets[nbi]]++;
									}
									else if(s instanceof State){ 
										int r = ((State) s).getRank();
										cnts[r%nbuckets[nbi]]++;
									}
								}
								for(int bi=0;bi<buckets.length;bi++){
									buckets[bi]= new Object[cnts[bi]];
								}
								
								int[] cnts2= new int[nbuckets[nbi]];
								for(Object s:samples){
									if(s==null)
										continue;
									if(s instanceof Person){
										int nc = ((Person) s).getCoffee();
										buckets[nc%nbuckets[nbi]][cnts2[nc%nbuckets[nbi]]++]= s;
									}
									else if(s instanceof State){
										int r = ((State) s).getRank();
										buckets[r%nbuckets[nbi]][cnts2[r%nbuckets[nbi]]++]= s;
									}
								}
								Estimator[] ests= new Estimator[nbuckets[nbi]];
								for(int ei=0;ei<ests.length;ei++){
									ests[ei] = new Estimator(buckets[ei]);
								}					

								System.out.print(""+nbuckets[nbi]+" "+samples.size());
								for(int pi=0;pi<ests.length;pi++){
									System.out.print(" "+cnts[pi]+" "+ests[pi].sum()); //count(*)	
								}
								System.out.println();
							}
						}
					}
				}
				db.close();
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
