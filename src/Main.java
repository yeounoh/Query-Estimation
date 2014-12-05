import java.awt.List;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;

// https://github.com/yeounoh/Query-Estimation.git
public class Main {
	
	/**
	 * Create a base table of distinct samples from a uniform distribution.
	 * This can be later resampled (under/over) to populate samples of interest.
	 */
	public static Database genUniform(String db_name, String table_name, boolean do_gen) throws Exception{
		Database db = new Database();
		db.connect(db_name);
	
		// create a base table (to be resampled) from a uniform distribution.
		if(do_gen){
			db.drop();
			db.connect(db_name);
			DataGenerator gen = new DataGenerator();
	
			//uniform
			db.createGDPTable(table_name); 
			gen.genUniform(db, table_name);
		}
		
		return db;
	}
	
	public static Database genGDPwiki(String db_name, String table_name, boolean do_gen) throws Exception{
		Database db = new Database();
		db.connect(db_name);
	
		//generate data (samples) from population
		if(do_gen){
			db.drop();
			db.connect(db_name);
			DataGenerator gen = new DataGenerator();
	
			//Wikipedia
			db.createGDPTable(table_name); 
			gen.loadGDPwiki(db, table_name);
			//System.out.println("sum for " + table_name + " is 14387583"); //ground truth
		}
		
		return db;
	}
	
	public static Database genGDPamt(String db_name, String table_name, boolean do_gen) throws Exception{
		Database db = new Database();
		db.connect(db_name);
	
		//generate data (samples) from population
		if(do_gen){
			db.drop();
			db.connect(db_name);
			DataGenerator gen = new DataGenerator();
			
			//AMT
			db.createHITTable(table_name); 
			gen.loadGDPamt(db, table_name);
			
			//Wikipedia
			db.createGDPTable("wiki"); 
			gen.loadGDPwiki(db, "wiki");
			
			// assume all answers contain the precise gdp values (no variations)
			db.queryExec("create table amt_t as (select assign_id, worker_id, hit_id, "
					+ "accept_t, state, wiki.gdp from amt left join wiki on amt.state=wiki.name)");
			db.queryExec("drop table amt");
			db.queryExec("create table amt as (select * from amt_t)");
			db.queryExec("drop table amt_t");
		}
		
		return db;
	}
	
	public static void main(String[] args){
		try{
			boolean gdp_amt = false;
			boolean gdp_wiki = false;
			boolean uniform = true;
			
			// real data sets
			if(gdp_amt){
				//database configuration-2
				String db_name = "gdp";
				String table_name = "amt"; 
				int p_size = 499; // population size, N
				
				boolean do_gen = true; //generate new coffee data set?
				Database db = genGDPamt(db_name, table_name, do_gen);
				 
				//experiment configuration
				int[] s_size = new int[(p_size-20)/20];
				for(int i=0;i<s_size.length-1;i++)
					s_size[i] = 20 + i*20;
				s_size[s_size.length-1] = p_size;
				int[] nbuckets = {1, 2, 3, 4, 5};
				int bucket_type = 2; //1- equi-range, 2- equi-size
				
				//file writer				
				FileOutputStream fos1= new FileOutputStream("./result/"+table_name+"_naive.txt");
				BufferedWriter bw1= new BufferedWriter(new OutputStreamWriter(fos1));
				FileOutputStream fos2= new FileOutputStream("./result/"+table_name+"_bucket_type"+bucket_type+"_est.txt");
				BufferedWriter bw2= new BufferedWriter(new OutputStreamWriter(fos2));
				FileOutputStream fos3= new FileOutputStream("./result/"+table_name+"_bucket_type"+bucket_type+"_cnt.txt");
				BufferedWriter bw3= new BufferedWriter(new OutputStreamWriter(fos3));
				FileOutputStream fos4= new FileOutputStream("./result/"+table_name+"_bucket_type"+bucket_type+"_chao.txt");
				BufferedWriter bw4= new BufferedWriter(new OutputStreamWriter(fos4));
				bw1.write("#|n|chao92|sum|sum_f1|sum_f12|"); bw1.flush(); bw1.newLine();
				bw2.write("#|nbucket|n|sum|"); bw2.flush(); bw2.newLine();
				bw3.write("#|nbucket|n|n_1| ... |n_n|"); bw3.flush(); bw3.newLine();
				bw4.write("#|nbucket|n|n_1| ... |n_n|"); bw4.flush(); bw4.newLine();
				
				//estimate population statistics from samples (method1: naive)
				for(int i=0;i<s_size.length;i++){
					ArrayList<Object> samples = new ArrayList<Object>(); 
					Object[] sample = db.sampleAMT(s_size[i], p_size, table_name);
						for(Object h : sample)
							samples.add(h);
							
					Object[] samples_arr = new Object[samples.size()];
					Object[] temp = samples.toArray();
					for(int ii=0;ii<samples_arr.length;ii++){
						samples_arr[ii] = temp[ii];
					}
					Estimator est = new Estimator(samples_arr);	
					bw1.write(""+samples.size()+" "+est.chao92()+" "+est.sum()+" "+est.sumf1()+" "+est.sumf12());
					bw1.flush();
					bw1.newLine();
				}
				
				//estimate population statistics from samples (method2: bucket)
				double[][][] est_by_bucket = new double[s_size.length][nbuckets.length][]; //sample size, bucket number, bucket idx
				double[][][] cnt_by_bucket = new double[s_size.length][nbuckets.length][];
				double[][][] chao_by_bucket = new double[s_size.length][nbuckets.length][];
				for(int i=0;i<s_size.length;i++){
					ArrayList<Object> samples = new ArrayList<Object>(); 
					Object[] sample = db.sampleAMT(s_size[i], p_size, table_name);
					if(bucket_type == 2)
						new QuickSort().quickSort(sample,0,sample.length-1); //equi-size bucket
					for(Object h : sample)
						samples.add(h);
							
					for(int nbi=0;nbi<nbuckets.length;nbi++){
						//create buckets
						Bucket[] buckets = new Bucket[nbuckets[nbi]];
						
						//equi-range bucket
						if(bucket_type == 1){
							int width = 2000000/buckets.length; //GDP2009- 2000000, GDP2012- 2500000
							for(int ii=0;ii<nbuckets[nbi];ii++){
								buckets[ii] = new Bucket(ii*width, (ii+1)*width);
							}
						}
						else if(bucket_type == 2){
							//equi-size bucket
							for(int ii=0;ii<nbuckets[nbi];ii++){ 
								int rem= 0, b_size = samples.size()/nbuckets[nbi]; 
								if(ii==(nbuckets[nbi]-1))
									rem = samples.size()%nbuckets[nbi];
								buckets[ii] = new Bucket(samples.subList(ii*b_size, (ii+1)*b_size + rem));
							}
						}
						
						int cnt = 0;
						for(Object s:samples){
							if(s==null)
								continue;
							if(s instanceof HIT){
								int gdp = ((HIT) s).getGDP();
								for(Bucket b : buckets){
									if(bucket_type == 1)
										if(b.getLowerB() <= gdp && b.getUpperB() >= gdp){
											b.insertGDP(s);
											break;
										}
									else if(bucket_type == 2)
										if(b.getLowerB() <= cnt && b.getUpperB() >= cnt){
											b.insertGDP(s);
											cnt++;
											break;
										}
								}
							}
						}
						
						cnt_by_bucket[i][nbi] = new double[nbuckets[nbi]];
						est_by_bucket[i][nbi] = new double[nbuckets[nbi]];
						chao_by_bucket[i][nbi] = new double[nbuckets[nbi]];
						for(int ii=0;ii<nbuckets[nbi];ii++){
							Object[] samples_b = buckets[ii].getSamples().toArray();
							Estimator est = new Estimator(samples_b);
							cnt_by_bucket[i][nbi][ii] = samples_b.length;
							est_by_bucket[i][nbi][ii] = est.sum();
							chao_by_bucket[i][nbi][ii] = est.chao92();
						}					
					}
				}
				
				for(int j=0;j<nbuckets.length;j++){
					for(int i=0;i<s_size.length;i++){
						double sum_t = 0.0;
						String cnt_t = "";
						String chao_t = "";
						for(int k=0;k<nbuckets[j];k++){
							sum_t += est_by_bucket[i][j][k];
							cnt_t += cnt_by_bucket[i][j][k] + " ";
							chao_t += chao_by_bucket[i][j][k] + " ";
						} 
						bw2.write(""+nbuckets[j]+" "+s_size[i]+" "+sum_t);
						bw3.write(""+nbuckets[j]+" "+s_size[i]+" "+cnt_t);
						bw4.write(""+nbuckets[j]+" "+s_size[i]+" "+chao_t);
						bw2.flush(); bw2.newLine();
						bw3.flush(); bw3.newLine();
						bw4.flush(); bw4.newLine();
					}
				}
				db.close();
				bw1.close(); bw2.close(); bw3.close(); bw4.close();
			}
			
			// synthetic data sets
			if(gdp_wiki || uniform){
				//database configuration-2
				boolean do_gen = true;
				String db_name = null;
				String table_name = null; 
				int p_size = 0; // ground truth (# of species, S)
				Database db = null;
				
				if(gdp_wiki){
					db_name = "gdp";
					table_name = "wiki"; 
					p_size = 50; // ground truth (# of species, S)
					db = genGDPwiki(db_name, table_name, do_gen);
				}
				else if(uniform){
					db_name = "synthetic";
					table_name = "uniform"; 
					p_size = 100;
					db = genUniform(db_name, table_name, do_gen);
				}
				 
				//experiment configuration
				int[] s_size = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25}; 
				int[] n_worker = {20}; // number of workers
				int sampling_type = 2; //Sampling method: 1- with replacement, 2- without replacement
				int[] nbuckets = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
				int bucket_type = 2; //1- equi-range, 2- equi-size
				int bucket_max_width = 0;
				if(gdp_wiki)
					bucket_max_width = 2000000; //GDP2009: 2000000, GDP2012: 2500000
				else if(uniform)
					bucket_max_width = 100; 
				double[] lamda = {0.1, 0.5, 1.0}; //correlation factor
				
				//file writer				
				FileOutputStream fos1= new FileOutputStream("./result/"+table_name+"_naive.txt");
				BufferedWriter bw1= new BufferedWriter(new OutputStreamWriter(fos1));
				FileOutputStream fos1r= new FileOutputStream("./result/"+table_name+"_naive_rep.txt");
				BufferedWriter bw1r= new BufferedWriter(new OutputStreamWriter(fos1r));
				FileOutputStream fos1r_f1= new FileOutputStream("./result/"+table_name+"_estf1_rep.txt");
				BufferedWriter bw1r_f1= new BufferedWriter(new OutputStreamWriter(fos1r_f1));
				FileOutputStream fos1r_f12= new FileOutputStream("./result/"+table_name+"_estf12_rep.txt");
				BufferedWriter bw1r_f12= new BufferedWriter(new OutputStreamWriter(fos1r_f12));
				
				FileOutputStream fos2= new FileOutputStream("./result/"+table_name+"_bucket_type"+bucket_type+"_est.txt");
				BufferedWriter bw2= new BufferedWriter(new OutputStreamWriter(fos2));
				FileOutputStream fos3= new FileOutputStream("./result/"+table_name+"_bucket_type"+bucket_type+"_cnt.txt");
				BufferedWriter bw3= new BufferedWriter(new OutputStreamWriter(fos3));
				FileOutputStream fos4= new FileOutputStream("./result/"+table_name+"_bucket_type"+bucket_type+"_chao.txt");
				BufferedWriter bw4= new BufferedWriter(new OutputStreamWriter(fos4));
				FileOutputStream fos5= new FileOutputStream("./result/"+table_name+"_bucket_type"+bucket_type+"_rep.txt");
				BufferedWriter bw5= new BufferedWriter(new OutputStreamWriter(fos5));
				bw1.write("#|lam|nworker|n|chao92|sum|sum_f1|sum_f12|"); bw1.flush(); bw1.newLine();
				bw1r.write("#|lam|nworker|n|avg|std|"); bw1r.flush(); bw1r.newLine();
				bw2.write("#|lam|nworker|nbucket|n|sum|"); bw2.flush(); bw2.newLine();
				bw3.write("#|lam|nworker|nbucket|n|n_1| ... |n_n|"); bw3.flush(); bw3.newLine();
				bw4.write("#|lam|nworker|nbucket|n|n_1| ... |n_n|"); bw4.flush(); bw4.newLine();
				bw5.write("#|lam|nworker|nbucket|n|avg|std|"); bw5.flush(); bw5.newLine();
				
				//estimate population statistics from samples (method1: naive)
				for(int cri=0;cri<lamda.length;cri++){
					for(int j=0;j<n_worker.length;j++){
						double[][] est_rep = new double[s_size.length][20]; //20 repetition
						double[][] estf1_rep = new double[s_size.length][20];
						double[][] estf12_rep = new double[s_size.length][20];
						for(int rep=0;rep<20;rep++){
							for(int i=0;i<s_size.length;i++){
								ArrayList<Object> samples = new ArrayList<Object>(); 
								for(int jj=0;jj<n_worker[j];jj++){
									Object[] sample = db.sample(s_size[i]*3, p_size, table_name, sampling_type);
									
									//resampling: likelihood of individual to be selected
									ArrayList<Object> resampled = db.resample(s_size[i], sample, lamda[cri]);
									Object[] resampled_arr = resampled.toArray();
									
									//Gathers all the samples collected by workers (worker arrival rates?) 
									for(Object s: resampled_arr)
										samples.add(s);
								}
								Object[] samples_arr = new Object[samples.size()];
								Object[] temp = samples.toArray();
								for(int ii=0;ii<samples_arr.length;ii++){
									samples_arr[ii] = temp[ii];
								}
								
								Estimator est = new Estimator(samples_arr);	
								bw1.write(""+lamda[cri]+" "+n_worker[j]+" "+samples.size()+" "+est.chao92()+" "+est.sum()+" "+est.sumf1()+" "+est.sumf12());
								bw1.flush();
								bw1.newLine();
								est_rep[i][rep] = est.sum();
								estf1_rep[i][rep] = est.sumf1();
								estf12_rep[i][rep] = est.sumf12();
							}
						}
						for(int i=0;i<s_size.length;i++){
							double std=0, avg=0, std_f1=0, avg_f1=0, std_f12=0, avg_f12=0;
							for(int ri=0;ri<20;ri++){
								avg += est_rep[i][ri];
								avg_f1 += estf1_rep[i][ri];
								avg_f12 += estf12_rep[i][ri];
							}
								
							avg = avg/20;
							avg_f1 = avg_f1/20;
							avg_f12 = avg_f12/20;
							for(int ri=0;ri<20;ri++){
								std += Math.sqrt((est_rep[i][ri]-avg)*(est_rep[i][ri]-avg)/20);
								std_f1 += Math.sqrt((estf1_rep[i][ri]-avg)*(estf1_rep[i][ri]-avg)/20);
								std_f12 += Math.sqrt((estf12_rep[i][ri]-avg)*(estf12_rep[i][ri]-avg)/20);
							}
							
							String est_t = ""+avg+" "+std;
							bw1r.write(""+lamda[cri]+" "+n_worker[j]+" "+" "+(s_size[i]*n_worker[j])+" "+est_t);
							bw1r.flush(); bw1r.newLine();
							
							String estf1_t = ""+avg_f1+" "+std_f1;
							bw1r_f1.write(""+lamda[cri]+" "+n_worker[j]+" "+" "+(s_size[i]*n_worker[j])+" "+estf1_t);
							bw1r_f1.flush(); bw1r_f1.newLine();
							
							String estf12_t = ""+avg_f12+" "+std_f12;
							bw1r_f12.write(""+lamda[cri]+" "+n_worker[j]+" "+" "+(s_size[i]*n_worker[j])+" "+estf12_t);
							bw1r_f12.flush(); bw1r_f12.newLine();
						}
					}
				}
				
				//estimate population statistics from samples (method2: bucket)
				for(int cri=0;cri<lamda.length;cri++){
					for(int wi=0;wi<n_worker.length;wi++){
						double[][][] est_rep = new double[s_size.length][nbuckets.length][20]; //20 repetition
						for(int rep=0;rep<20;rep++){
							double[][][] est_by_bucket = new double[s_size.length][nbuckets.length][];
							double[][][] cnt_by_bucket = new double[s_size.length][nbuckets.length][];
							double[][][] chao_by_bucket = new double[s_size.length][nbuckets.length][];
							for(int i=0;i<s_size.length;i++){
								ArrayList<Object> samples = new ArrayList<Object>(); 
								for(int jj=0;jj<n_worker[wi];jj++){
									Object[] sample = db.sample(s_size[i]*3, p_size, table_name, sampling_type);
									
									//resampling: likelihood of individual to be selected
									ArrayList<Object> resampled = db.resample(s_size[i], sample, lamda[cri]);
									
									//worker arrival rates?
									Iterator itr = resampled.iterator();
									while(itr.hasNext()){
										samples.add(itr.next());
									}
								}
								Object[] samples_arr = samples.toArray();
								if(bucket_type == 2)
									new QuickSort().quickSort(samples_arr,0,samples_arr.length-1); //equi-size bucket
								
								for(int nbi=0;nbi<nbuckets.length;nbi++){//create buckets
									Bucket[] buckets = new Bucket[nbuckets[nbi]];
									
									//equi-range bucket
									if(bucket_type == 1){
										int width = bucket_max_width/buckets.length; 
										for(int ii=0;ii<nbuckets[nbi];ii++){ 
											buckets[ii] = new Bucket(ii*width, (ii+1)*width);
										}
									} 
									else if(bucket_type == 2){
										//equi-size bucket
										for(int ii=0;ii<nbuckets[nbi];ii++){ 
											int rem= 0, b_size = samples.size()/nbuckets[nbi];
											if(ii==nbuckets[nbi]-1)
												rem = samples.size()%nbuckets[nbi];
											buckets[ii] = new Bucket(ii*b_size, (ii+1)*b_size + rem);
											//System.out.println("["+(ii*b_size)+", "+((ii+1)*b_size + rem)+"]");
										}
									}
									
									int cnt = 0;
									for(Object s:samples_arr){
										if(s==null)
											continue;
										if(s instanceof State){
											int gdp = ((State) s).getGDP();
											for(Bucket b : buckets){
												if(bucket_type == 1){
													if(b.getLowerB() <= gdp && b.getUpperB() >= gdp){
														b.insertGDP(s);
														break;
													}
												}
												else if(bucket_type == 2){
													if(b.getLowerB() <= cnt && b.getUpperB() >= cnt){
														b.insertGDP(s);
														cnt++;
														break;
													}
												}
											}
										}
									}
									
									cnt_by_bucket[i][nbi] = new double[nbuckets[nbi]];
									chao_by_bucket[i][nbi] = new double[nbuckets[nbi]];
									est_by_bucket[i][nbi] = new double[nbuckets[nbi]];
									double sum_t = 0.0;
									for(int ii=0;ii<nbuckets[nbi];ii++){
										Object[] samples_b = buckets[ii].getSamples().toArray();
										Estimator est = new Estimator(samples_b);
										//System.out.println("bucket:"+ii+", size:"+samples_b.length);
										cnt_by_bucket[i][nbi][ii] = samples_b.length; 
										est_by_bucket[i][nbi][ii] = est.sum();
										sum_t += est_by_bucket[i][nbi][ii];
										chao_by_bucket[i][nbi][ii] = est.chao92();
									}
									est_rep[i][nbi][rep] = sum_t; 
								}
							}
							//contains the last run's results
							for(int j=0;j<nbuckets.length;j++){
								for(int i=0;i<s_size.length;i++){
									double sum_t = 0.0;
									String cnt_t = "";
									String chao_t = "";
									for(int k=0;k<nbuckets[j];k++){
										sum_t += est_by_bucket[i][j][k];
										cnt_t += cnt_by_bucket[i][j][k] + " ";
										chao_t += chao_by_bucket[i][j][k] + " ";
									} 
									bw2.write(""+lamda[cri]+" "+n_worker[wi]+" "+nbuckets[j]+" "+(s_size[i]*n_worker[wi])+" "+sum_t);
									bw3.write(""+lamda[cri]+" "+n_worker[wi]+" "+nbuckets[j]+" "+(s_size[i]*n_worker[wi])+" "+cnt_t);
									bw4.write(""+lamda[cri]+" "+n_worker[wi]+" "+nbuckets[j]+" "+(s_size[i]*n_worker[wi])+" "+chao_t);
									bw2.flush(); bw2.newLine();
									bw3.flush(); bw3.newLine();
									bw4.flush(); bw4.newLine();
								}
							}
						}
						
						for(int j=0;j<nbuckets.length;j++){
							for(int i=0;i<s_size.length;i++){
								double std=0, avg=0;
								for(double v:est_rep[i][j])
									avg += v;
								avg = avg/est_rep[i][j].length;
								for(double v:est_rep[i][j])
									std += Math.sqrt((v-avg)*(v-avg)/est_rep[i][j].length);
								
								String est_t = ""+avg+" "+std;
								bw5.write(""+lamda[cri]+" "+n_worker[wi]+" "+nbuckets[j]+" "+(s_size[i]*n_worker[wi])+" "+est_t);
								bw5.flush(); bw5.newLine();
							}
						}
					}
				}
				db.close();
				bw1.close();bw2.close();bw3.close();bw4.close();bw5.close();
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
