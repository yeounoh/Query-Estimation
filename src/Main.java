import java.awt.List;
import java.util.ArrayList;


public class Main {

	//to-do: need re-factoring
	public static void main(String[] args){
		try{
			String db_name = "coffee";
			String[] population_name = {"male"}; //{"male", "female"};
			int p_size = 5000; 
			int[] s_size = {250, 500, 1000, 2000, 3000, 4000, 5000, 6000};
			int[] n_itr = {1}; //{1, 2, 3};
			
			Database db = new Database(); 
			db.connect(db_name); //if "coffee" is not present, then create a new database
			
			//generate data (samples) from population
			boolean do_gen = false;
			if(do_gen){
				db.drop();
				db.connect(db_name);
				DataGenerator gen = new DataGenerator();
				Aggregation agg = new Aggregation();
				for(int i=0;i<population_name.length;i++){
					String t_name = population_name[i];
					db.createTable(t_name); 
					gen.generateData(db, t_name, p_size, p_size*i); //male coffee consumption: Gaussian
					System.out.println("sum for "+t_name+" is "+agg.sum(db.selectAll(t_name)));
				}
			}
			
			//estimate population statistics from samples (method1)
			for(int i=0;i<s_size.length;i++){
				for(int j=0;j<n_itr.length;j++){
					ArrayList<Person> samples = new ArrayList<Person>(); //[s_size[i]*n_itr[j]*population_name.length];
					
					for(int jj=0;jj<n_itr[j];jj++){
						for(int k=0;k<population_name.length;k++){
							Person[] sample = db.sample(s_size[i], p_size, population_name[k], 1);
							for(int z=0;z<sample.length;z++) //union of sample tables
								samples.add(sample[z]);
						}
					}
					
					Estimator est = new Estimator((Person[]) samples.toArray());
					//System.out.println(" "+population_name.length+"*"+n_itr[j]+"*"+s_size[i]+" "+est.chao92()+" "+est.chao84()); //count(*)	
					System.out.println(" "+population_name.length+"*"+n_itr[j]+"*"+s_size[i]+" "+est.chao92()+" "+est.sum()); //sum(*)
				}
			}
			
			//estimate population statistics from samples (method2)
//			for(int i=0;i<s_size.length;i++){
//				for(int j=0;j<n_itr.length;j++){
//					Person[] samples = new Person[s_size[i]*n_itr[j]*population_name.length];
//					int idx= 0;
//					for(int jj=0;jj<n_itr[j];jj++){
//						for(int k=0;k<population_name.length;k++){
//							Person[] sample = db.sample(s_size[i], p_size, population_name[k], 1);
//							for(int z=0;z<sample.length;z++)
//								samples[idx++] = sample[z];
//						}
//					}
//					
//					int[] cnts= new int[5]; //# cups of coffee
//					Person[][] buckets = new Person[5][];;
//					for(Person s:samples){
//						if(s==null)
//							continue;
//						int nc = s.getCoffee();
//						cnts[nc%5]++;
//					}
//					for(int bi=0;bi<buckets.length;bi++){
//						buckets[bi]= new Person[cnts[bi]];
//					}
//					
//					int[] cnts2= new int[5];
//					for(Person s:samples){
//						if(s==null)
//							continue;
//						
//						int nc = s.getCoffee();
//						buckets[nc%5][cnts2[nc%5]++]= s;
//					}
//					Estimator[] ests= new Estimator[5];
//					for(int ei=0;ei<ests.length;ei++){
//						ests[ei] = new Estimator(buckets[ei]);
//					}					
//					
//					System.out.print(""+population_name.length+"*"+n_itr[j]+"*"+s_size[i]);
//					for(int pi=0;pi<ests.length;pi++){
//						System.out.print(" "+cnts[pi]+" "+ests[pi].sum()); //count(*)	
//					}
//					System.out.println();
//				}
//			}
			
			db.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
