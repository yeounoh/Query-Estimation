import java.awt.List;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;

// https://github.com/yeounoh/Query-Estimation.git
public class Main {
	
	/**
	 * 
	 * @param db_name
	 * @param table_name
	 * @param do_gen
	 * @param type
	 * @return
	 * @throws Exception
	 */
	public static Database genSynthetic(String db_name, String table_name, boolean do_gen, int type) throws Exception{
		Database db = new Database();
		db.connect(db_name);
	
		//generate data (samples) from population
		if(do_gen){
			db.drop();
			db.connect(db_name);
			DataGenerator gen = new DataGenerator();
	
			db.createSyntTable(table_name); 
			if(type == 1){
				//Wikipedia
				gen.loadSyntheticData(db, table_name, 1);
				System.out.println("sum for " + table_name + " is 14387583"); //ground truth
			}
			else if(type == 2){
				//1~100
				gen.loadSyntheticData(db, table_name, 2);
				System.out.println("sum for " + table_name + " is 5050");
			}
		}
		
		return db;
	}
	
	/**
	 * 
	 * @param db_name
	 * @param table_name
	 * @param do_gen
	 * @param type 1: GDP, 2: Solar
	 * @return database object to connect to mysql db
	 * @throws Exception
	 */
	public static Database genRealAMT(String db_name, String table_name, boolean do_gen, int type) throws Exception{
		Database db = new Database();
		db.connect(db_name);
	
		//generate data (samples) from population
		if(do_gen){
			db.drop();
			db.connect(db_name);
			DataGenerator gen = new DataGenerator();
			
			//AMT
			db.createHITTable(table_name); 
			gen.loadRealAMT(db, table_name,type); //GDP
			
			if(type == 1){
				//Wikipedia
				db.createSyntTable("wiki"); 
				gen.loadSyntheticData(db, "wiki", 1);
				
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
	
	public static void main(String[] args){
		try{
			//experimental parameters
			boolean amt_data = false;
			boolean synt_data = true;
			int synt_data_type = 1; //1: wiki, 2: uniform
			int n_rep = 200; //synthetic data experiment
			int amt_data_type = 2; //1: GDP, 2: Solar
			boolean do_gen = true;
			
			if(amt_data){
				//database configuration-2
				String db_name = "gdp";
				String table_name = "amt";
				
				int p_size = 0;
				Database db = null;
				if(amt_data_type == 1){
					p_size = 499; // number of samples (GDP)
					db = genRealAMT(db_name, table_name, do_gen, 1); //1:GDP, 2:Solar
				}
				else if(amt_data_type == 2){
					p_size = 327; // number of samples (Solar)
					db = genRealAMT(db_name, table_name, do_gen, 2); //1:GDP, 2:Solar
				}
				 
				//experiment configuration
				int[] s_size = new int[(p_size-20)/20];
				for(int i=0;i<s_size.length-1;i++)
					s_size[i] = 20 + i*20;
				s_size[s_size.length-1] = p_size;
				int[] nbuckets = {1,2,3,4,5};
				int bucket_type = 1; //1- equi-range(auto), 2- equi-size, 3- equi-range
				int estimator_type = 1; // 1- chao92, 2- f1
				double thresh_c = 0.9; //how do we define this magic number?
				double thresh_cv = 0.05;
				
				//file writer				
				FileOutputStream fos1= new FileOutputStream("./result/amt_naive.txt");
				BufferedWriter bw1= new BufferedWriter(new OutputStreamWriter(fos1));
				String bucket_est_output = "./result/amt_bucket_type" + bucket_type;
				bucket_est_output = estimator_type == 1 ? bucket_est_output + "_est.txt" : "_f1_est.txt";
				FileOutputStream fos2= new FileOutputStream(bucket_est_output);
				BufferedWriter bw2= new BufferedWriter(new OutputStreamWriter(fos2));
				FileOutputStream fos3= new FileOutputStream("./result/amt_bucket_type"+bucket_type+"_cnt.txt");
				BufferedWriter bw3= new BufferedWriter(new OutputStreamWriter(fos3));
				FileOutputStream fos4= new FileOutputStream("./result/amt_bucket_type"+bucket_type+"_chao.txt");
				BufferedWriter bw4= new BufferedWriter(new OutputStreamWriter(fos4));
				FileOutputStream fos5= new FileOutputStream("./result/amt_bucket_type"+bucket_type+"_c.txt");
				BufferedWriter bw5= new BufferedWriter(new OutputStreamWriter(fos5));
				bw1.write("#|n|chao92|sum|sum_f1|sum_f12|sampCov|n_uniq|"); bw1.flush(); bw1.newLine();
				bw2.write("#|nbucket|n|sum|avg_c|"); bw2.flush(); bw2.newLine();
				bw3.write("#|nbucket|n|n_1| ... |n_n|"); bw3.flush(); bw3.newLine();
				bw4.write("#|nbucket|n|n_1| ... |n_n|"); bw4.flush(); bw4.newLine();
				bw5.write("#|nbucket|n|n_1| ... |n_n|"); bw5.flush(); bw5.newLine();
				
				//estimate population statistics from samples (method1: naive)
				for(int i=0;i<s_size.length;i++){
					ArrayList<Object> samples = new ArrayList<Object>(); 
					Object[] sample = db.sampleAMT(s_size[i], table_name);
						for(Object h : sample)
							samples.add(h);
							
					Object[] samples_arr = new Object[samples.size()];
					Object[] temp = samples.toArray();
					for(int ii=0;ii<samples_arr.length;ii++){
						samples_arr[ii] = temp[ii];
					}
					Estimator est = new Estimator(samples_arr);	
					bw1.write(""+samples.size()+" "+est.chao92()+" "+est.sum()+" "+est.sumf1()+" "+est.sumf12()+" "+est.sampleCov()+" "+est.getUniqueCount());
					bw1.flush();
					bw1.newLine();
				}
				
				//estimate population statistics from samples (method2: bucket)
				int bucket_sizes = bucket_type == 1? 1 : nbuckets.length;
				
				int[] auto_b_size = new int[s_size.length];
				double[][][] est_by_bucket = new double[s_size.length][bucket_sizes][]; //sample size, bucket number, bucket idx
				double[][][] cnt_by_bucket = new double[s_size.length][bucket_sizes][];
				double[][][] chao_by_bucket = new double[s_size.length][bucket_sizes][];
				double[][][] c_by_bucket = new double[s_size.length][bucket_sizes][];
				
				for(int i=0;i<s_size.length;i++){
					ArrayList<Object> samples = new ArrayList<Object>(); 
					Object[] sample = db.sampleAMT(s_size[i], table_name);
					if(bucket_type == 2)
						new QuickSort().quickSort(sample,0,sample.length-1); //equi-size bucket
					for(Object h : sample)
						samples.add(h);
					
					for(int nbi=0;nbi<bucket_sizes;nbi++){
						//create buckets
						Bucket[] buckets = null;
						if(bucket_type == 1){
							buckets = new Estimator(samples.toArray()).autoBuckets(thresh_c, thresh_cv,samples.toArray());
							auto_b_size[i] = buckets.length;
						}
						else {
							buckets = new Bucket[nbuckets[nbi]];
							
							if(bucket_type == 2){
								//equi-size bucket
								for(int ii=0;ii<nbuckets[nbi];ii++){ 
									int rem= 0, b_size = samples.size()/nbuckets[nbi]; 
									if(ii==(nbuckets[nbi]-1))
										rem = samples.size()%nbuckets[nbi];
									buckets[ii] = new Bucket(samples.subList(ii*b_size, (ii+1)*b_size + rem).toArray());
								}
							}
							//equi-range bucket *how do we automatically adjust the number of bucket/size?
							else if(bucket_type == 3){
								double width = 2000000/buckets.length; //GDP2009- 2000000, GDP2012- 2500000
								for(int ii=0;ii<nbuckets[nbi];ii++){
									buckets[ii] = new Bucket(ii*width, (ii+1)*width);
								}
							}
						}
						
						int cnt = 0;
						for(Object s:samples){
							if(s==null)
								continue;
							if(s instanceof HIT){
								double value = ((HIT) s).getValue();
								for(Bucket b : buckets){
									if(bucket_type == 1 || bucket_type == 3)
										if(b.getLowerB() <= value && b.getUpperB() >= value){
											b.insertSample(s);
											break;
										}
									else if(bucket_type == 2)
										if(b.getLowerB() <= cnt && b.getUpperB() >= cnt){
											b.insertSample(s);
											cnt++;
											break;
										}
								}
							}
						}
						
						cnt_by_bucket[i][nbi] = new double[buckets.length];
						est_by_bucket[i][nbi] = new double[buckets.length];
						chao_by_bucket[i][nbi] = new double[buckets.length];
						c_by_bucket[i][nbi] = new double[buckets.length];
						for(int ii=0;ii<buckets.length;ii++){
							Object[] samples_b = buckets[ii].getSamples().toArray();
							Estimator est = new Estimator(samples_b);
							cnt_by_bucket[i][nbi][ii] = samples_b.length;
							est_by_bucket[i][nbi][ii] = estimator_type == 1 ? est.sum() : est.sumf1();
							chao_by_bucket[i][nbi][ii] = est.chao92();
							c_by_bucket[i][nbi][ii] = buckets[ii].getSampleCov();
						}					
					}
				}
				
				for(int j=0;j<bucket_sizes;j++){
					for(int i=0;i<s_size.length;i++){
						double sum_t = 0.0;
						double avg_c = 0.0;
						String cnt_t = "";
						String chao_t = "";
						String c_t = "";
						
						int b_size = bucket_type == 1? auto_b_size[i] : nbuckets[j];
						for(int k=0;k<b_size;k++){
							sum_t += est_by_bucket[i][j][k];
							avg_c += c_by_bucket[i][j][k] * (double) cnt_by_bucket[i][j][k]/(double) s_size[i];
							cnt_t += cnt_by_bucket[i][j][k] + " ";
							chao_t += chao_by_bucket[i][j][k] + " ";
							c_t += c_by_bucket[i][j][k] + " ";
						} 
						
						bw2.write(""+b_size+" "+s_size[i]+" "+sum_t+" "+avg_c);
						bw3.write(""+b_size+" "+s_size[i]+" "+cnt_t);
						bw4.write(""+b_size+" "+s_size[i]+" "+chao_t);
						bw5.write(""+b_size+" "+s_size[i]+" "+c_t);
						bw2.flush(); bw2.newLine();
						bw3.flush(); bw3.newLine();
						bw4.flush(); bw4.newLine();
						bw5.flush(); bw5.newLine();
					}
				}
				db.close();
				bw1.close(); bw2.close(); bw3.close(); bw4.close(); bw5.close();
			}
			
			if(synt_data){
				//database configuration-2
				String db_name = "synt";
				String table_name = "wiki"; 
				int n_class = 0; // ground truth for C (# of species), used for random sample generation
				
				Database db = null;
				if(synt_data_type == 1){
					db = genSynthetic(db_name, table_name, do_gen, 1); // type = 1, wikipedia gdp
					n_class = 50;
				}
				else if(synt_data_type == 2){
					db = genSynthetic(db_name, table_name, do_gen, 2); // type = 2, uniform (1~100)
					n_class = 100; //also modify the max parameter in DataGenerator class
				}
				 
				//experiment configuration
				
				int[] s_size = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25}; //{1,4,7,10,13,16,19,22,25,28,31,34,37,40,43,46,49}; //
				int[] n_worker = {20}; // number of workers
				int sampling_type = synt_data_type == 2 ? 1 : 2; //1- with replacement, 2- without replacement
				int[] nbuckets = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
				int bucket_type = 1; //1- equi-range(auto), 2- equi-size, 3- equi-range
				int estimator_type = 1; //1- chao, 2- f1
				double thresh_c = 0.9; //how do we define this magic number?
				double thresh_cv = 0.05; 
				double cf = synt_data_type == 2 ? 0 : 0.5;
				double[] lamda = {cf}; //correlation factor
				
				//file writer				
				FileOutputStream fos1= new FileOutputStream("./result/wiki_naive.txt");
				BufferedWriter bw1= new BufferedWriter(new OutputStreamWriter(fos1));
				FileOutputStream fos1r= new FileOutputStream("./result/wiki_naive_rep.txt");
				BufferedWriter bw1r= new BufferedWriter(new OutputStreamWriter(fos1r));
				FileOutputStream fos1r_f1= new FileOutputStream("./result/wiki_estf1_rep.txt");
				BufferedWriter bw1r_f1= new BufferedWriter(new OutputStreamWriter(fos1r_f1));
				FileOutputStream fos1r_f12= new FileOutputStream("./result/wiki_estf12_rep.txt");
				BufferedWriter bw1r_f12= new BufferedWriter(new OutputStreamWriter(fos1r_f12));
				FileOutputStream fos1r_c= new FileOutputStream("./result/wiki_naive_c_rep.txt");
				BufferedWriter bw1r_c= new BufferedWriter(new OutputStreamWriter(fos1r_c));
				
				String bucket_est_output = "./result/wiki_bucket_type"+bucket_type;
				bucket_est_output = estimator_type == 1 ? bucket_est_output+"_est.txt" : bucket_est_output+"_f1_est.txt";
				FileOutputStream fos2= new FileOutputStream(bucket_est_output);
				BufferedWriter bw2= new BufferedWriter(new OutputStreamWriter(fos2));
				FileOutputStream fos3= new FileOutputStream("./result/wiki_bucket_type"+bucket_type+"_cnt.txt");
				BufferedWriter bw3= new BufferedWriter(new OutputStreamWriter(fos3));
				FileOutputStream fos4= new FileOutputStream("./result/wiki_bucket_type"+bucket_type+"_chao.txt");
				BufferedWriter bw4= new BufferedWriter(new OutputStreamWriter(fos4));
				String bucket_rep_output = "./result/wiki_bucket_type"+bucket_type;
				bucket_rep_output = estimator_type == 1 ? bucket_rep_output+"_rep.txt" : bucket_rep_output+"_f1_rep.txt";
				FileOutputStream fos5= new FileOutputStream(bucket_rep_output);
				BufferedWriter bw5= new BufferedWriter(new OutputStreamWriter(fos5));
				
				bw1.write("#|lam|nworker|n|chao92|sum|sum_f1|sum_f12|"); bw1.flush(); bw1.newLine();
				bw1r.write("#|lam|nworker|n|avg|std|avg_c|avg_cv|n_uniq|"); bw1r.flush(); bw1r.newLine(); //
				bw1r_f1.write("#|lam|nworker|n|avg_f1|std|"); bw1r_f1.flush(); bw1r_f1.newLine();
				bw1r_f12.write("#|lam|nworker|n|avg_f12|std|"); bw1r_f12.flush(); bw1r_f12.newLine();
				
				bw2.write("#|lam|nworker|nbucket|n|sum|"); bw2.flush(); bw2.newLine();
				bw3.write("#|lam|nworker|nbucket|n|n_1| ... |n_n|"); bw3.flush(); bw3.newLine();
				bw4.write("#|lam|nworker|nbucket|n|n_1| ... |n_n|"); bw4.flush(); bw4.newLine();
				bw5.write("#|lam|nworker|nbucket|n|avg|std|avg_c|avg_cv|"); bw5.flush(); bw5.newLine();
				
				for(int cri=0;cri<lamda.length;cri++){
					for(int wi=0;wi<n_worker.length;wi++){
						//estimate population statistics from samples (method1: naive)
						double[][] est_rep = new double[s_size.length][n_rep];
						double[][] estf1_rep = new double[s_size.length][n_rep];
						double[][] estf12_rep = new double[s_size.length][n_rep];
						double[][] estc_rep = new double[s_size.length][n_rep];
						double[][] estcv_rep = new double[s_size.length][n_rep];
						int[][] uniq_rep = new int[s_size.length][n_rep];
						
						//estimate population statistics from samples (method2: bucket)
						double[][][] est_rep_bucket = new double[s_size.length][nbuckets.length][n_rep]; 
						double[][][] c_rep_bucket = new double[s_size.length][nbuckets.length][n_rep];
						double[][][] cv_rep_bucket = new double[s_size.length][nbuckets.length][n_rep];
						int bucket_sizes = bucket_type == 1? 1 : nbuckets.length;
						int[][] auto_b_size_rep = new int[s_size.length][n_rep];
						for(int rep=0;rep<n_rep;rep++){
							System.out.print(".");
							
							//estimate population statistics from samples (method2: bucket)
							double[][][] est_by_bucket = new double[s_size.length][bucket_sizes][]; //sample size, bucket number, bucket idx
							double[][][] cnt_by_bucket = new double[s_size.length][bucket_sizes][];
							double[][][] chao_by_bucket = new double[s_size.length][bucket_sizes][];
							double[][][] c_by_bucket = new double[s_size.length][bucket_sizes][];
							double[][][] cv_by_bucket = new double[s_size.length][bucket_sizes][];
							
							for(int i=0;i<s_size.length;i++){
								ArrayList<Object> samples = new ArrayList<Object>(); 
								for(int jj=0;jj<n_worker[wi];jj++){
									Object[] sample = db.sample(s_size[i]*3, n_class, table_name, sampling_type);
									//resampling: likelihood of individual to be selected
									ArrayList<Object> resampled = db.resample(s_size[i], sample, lamda[cri], n_class);
									Object[] resampled_arr = resampled.toArray();
									//Gathers all the samples collected by workers (worker arrival rates?) 
									for(Object s: resampled_arr)
										samples.add(s);
								}
								Object[] samples_arr = samples.toArray();
								if(bucket_type == 2)
									new QuickSort().quickSort(samples_arr,0,samples_arr.length-1); //equi-size bucket
								
								//estimate population statistics from samples (method1: naive)
								Estimator est = new Estimator(samples_arr);	
								bw1.write(""+lamda[cri]+" "+n_worker[wi]+" "+samples.size()+" "+est.chao92()+" "+est.sum()+" "+est.sumf1()+" "+est.sumf12());
								bw1.flush();
								bw1.newLine();
								est_rep[i][rep] = est.sum();
								estf1_rep[i][rep] = est.sumf1();
								estf12_rep[i][rep] = est.sumf12();
								estc_rep[i][rep] = est.sampleCov();
								estcv_rep[i][rep] = est.coeffVar();
								uniq_rep[i][rep] = est.getUniqueCount();
								
								for(int nbi=0;nbi<bucket_sizes;nbi++){//create buckets
									//create buckets
									Bucket[] buckets = null;
									if(bucket_type == 1){
										buckets = new Estimator(samples.toArray()).autoBuckets(thresh_c, thresh_cv, samples.toArray());
										auto_b_size_rep[i][rep] = buckets.length;
									}
									else {
										buckets = new Bucket[nbuckets[nbi]];
										
										if(bucket_type == 2){
											//equi-size bucket
											for(int ii=0;ii<nbuckets[nbi];ii++){ 
												int rem= 0, b_size = samples.size()/nbuckets[nbi]; 
												if(ii==(nbuckets[nbi]-1))
													rem = samples.size()%nbuckets[nbi];
												buckets[ii] = new Bucket(samples.subList(ii*b_size, (ii+1)*b_size + rem).toArray());
											}
										}
										//equi-range bucket *how do we automatically adjust the number of bucket/size?
										else if(bucket_type == 3){
											double width = synt_data_type == 1? 2000000/buckets.length : n_class/buckets.length; //GDP2009- 2000000, GDP2012- 2500000
											for(int ii=0;ii<nbuckets[nbi];ii++){
												buckets[ii] = new Bucket(ii*width, (ii+1)*width);
											}
										}
										
										int cnt = 0; 
										for(Object s:samples){
											if(s==null)
												continue;
											if(s instanceof State){
												double value = ((State) s).getGDP();
												for(Bucket b : buckets){
													if(bucket_type == 1 || bucket_type == 3)
														if(b.getLowerB() <= value && b.getUpperB() >= value){
															b.insertSample(s);
															break;
														}
													else if(bucket_type == 2)
														if(b.getLowerB() <= cnt && b.getUpperB() >= cnt){
															b.insertSample(s);
															cnt++;
															break;
														}
												}
											}
										}
									}
									
									cnt_by_bucket[i][nbi] = new double[buckets.length];
									chao_by_bucket[i][nbi] = new double[buckets.length];
									est_by_bucket[i][nbi] = new double[buckets.length];
									c_by_bucket[i][nbi] = new double[buckets.length];
									cv_by_bucket[i][nbi] = new double[buckets.length];
									double sum_t = 0.0, avg_c = 0.0, avg_cv = 0.0;
									for(int ii=0;ii<buckets.length;ii++){
										Object[] samples_b = buckets[ii].getSamples().toArray();
										est = new Estimator(samples_b);
										cnt_by_bucket[i][nbi][ii] = samples_b.length; 
										est_by_bucket[i][nbi][ii] = estimator_type == 1 ? est.sum() : est.sumf1(); 
										sum_t += est_by_bucket[i][nbi][ii];
										c_by_bucket[i][nbi][ii] = buckets[ii].getSampleCov();
										cv_by_bucket[i][nbi][ii] = buckets[ii].getCoeffVar();
										avg_c += c_by_bucket[i][nbi][ii] * (double) samples_b.length/(double) s_size[i]/(double) n_worker[wi]; 
										avg_cv += cv_by_bucket[i][nbi][ii] * (double) samples_b.length/(double) s_size[i]/(double) n_worker[wi];
										chao_by_bucket[i][nbi][ii] = est.chao92();
									}
									est_rep_bucket[i][nbi][rep] = sum_t; 
									c_rep_bucket[i][nbi][rep] = avg_c;
									cv_rep_bucket[i][nbi][rep] = avg_cv;
								}
							}
							//contains the last run's results (
							for(int j=0;j<bucket_sizes;j++){
								for(int i=0;i<s_size.length;i++){
									double sum_t = 0.0;
									String cnt_t = "";
									String chao_t = "";
									//String c_t = "";
									
									int b_size = bucket_type == 1? auto_b_size_rep[i][rep] : nbuckets[j];
									for(int k=0;k<b_size;k++){
										sum_t += est_by_bucket[i][j][k];
										cnt_t += cnt_by_bucket[i][j][k] + " ";
										chao_t += chao_by_bucket[i][j][k] + " ";
										//c_t += c_by_bucket[i][j][k] + " ";
									} 
									bw2.write(""+lamda[cri]+" "+n_worker[wi]+" "+b_size+" "+s_size[i]+" "+sum_t);
									bw3.write(""+lamda[cri]+" "+n_worker[wi]+" "+b_size+" "+s_size[i]+" "+cnt_t);
									bw4.write(""+lamda[cri]+" "+n_worker[wi]+" "+b_size+" "+s_size[i]+" "+chao_t);
									//bw6.write(""+lamda[cri]+" "+n_worker[wi]+" "+b_size+" "+s_size[i]+" "+c_t);
									bw2.flush(); bw2.newLine();
									bw3.flush(); bw3.newLine();
									bw4.flush(); bw4.newLine();
									//bw6.flush(); bw6.newLine();
								}
							}
						}
						
						//naive
						for(int i=0;i<s_size.length;i++){
							double avg=0, avg_f1=0, avg_f12=0, avg_c=0, avg_cv=0, avg_uniq=0;
							for(int ri=0;ri<n_rep;ri++){
								avg += est_rep[i][ri];
								avg_f1 += estf1_rep[i][ri];
								avg_f12 += estf12_rep[i][ri];
								avg_c += estc_rep[i][ri];
								avg_cv += estcv_rep[i][ri];
								avg_uniq += (double) uniq_rep[i][ri];
							}
							avg = avg/n_rep;
							avg_f1 = avg_f1/n_rep;
							avg_f12 = avg_f12/n_rep;
							avg_c = avg_c/n_rep;
							avg_cv = avg_cv/n_rep;
							avg_uniq = avg_uniq/n_rep;
							
							double std=0, std_f1=0,std_f12=0;
							for(int ri=0;ri<n_rep;ri++){
								std += (est_rep[i][ri]-avg)*(est_rep[i][ri]-avg);
								std_f1 += (estf1_rep[i][ri]-avg)*(estf1_rep[i][ri]-avg);
								std_f12 += (estf12_rep[i][ri]-avg)*(estf12_rep[i][ri]-avg);
							}
							std = Math.sqrt(std/n_rep); std_f1 = Math.sqrt(std_f1/n_rep); std_f12 = Math.sqrt(std_f12/n_rep);
							
							String est_t = ""+avg+" "+std+" "+avg_c+" "+avg_cv+" "+avg_uniq;
							bw1r.write(""+lamda[cri]+" "+n_worker[wi]+" "+" "+(s_size[i]*n_worker[wi])+" "+est_t);
							bw1r.flush(); bw1r.newLine();
							
							String estf1_t = ""+avg_f1+" "+std_f1;
							bw1r_f1.write(""+lamda[cri]+" "+n_worker[wi]+" "+" "+(s_size[i]*n_worker[wi])+" "+estf1_t);
							bw1r_f1.flush(); bw1r_f1.newLine();
							
							String estf12_t = ""+avg_f12+" "+std_f12;
							bw1r_f12.write(""+lamda[cri]+" "+n_worker[wi]+" "+" "+(s_size[i]*n_worker[wi])+" "+estf12_t);
							bw1r_f12.flush(); bw1r_f12.newLine();
						}
						
						//bucket
						for(int j=0;j<bucket_sizes;j++){
							for(int i=0;i<s_size.length;i++){
								double std=0, avg=0, avg_b_size=0;
								for(double v:est_rep_bucket[i][j])
									avg += v;
								avg = avg/est_rep_bucket[i][j].length;
								for(double v:est_rep_bucket[i][j])
									std += (v-avg)*(v-avg);
								std = Math.sqrt(std/est_rep_bucket[i][j].length);
								for(int bs : auto_b_size_rep[i])
									avg_b_size += bs;
								avg_b_size = avg_b_size/auto_b_size_rep[i].length;
								
								double b_size = bucket_type == 1? avg_b_size : nbuckets[j];
								
								double c_avg=0;
								for(double v:c_rep_bucket[i][j])
									c_avg += v;
								c_avg = c_avg/c_rep_bucket[i][j].length;
								
								double cv_avg=0;
								for(double v:cv_rep_bucket[i][j])
									cv_avg += v;
								cv_avg = cv_avg/cv_rep_bucket[i][j].length;
								
								String est_t = ""+avg+" "+std+" "+c_avg+" "+cv_avg;
								bw5.write(""+lamda[cri]+" "+n_worker[wi]+" "+b_size+" "+(s_size[i]*n_worker[wi])+" "+est_t);
								bw5.flush(); bw5.newLine();
							}
						}
					}
				}
				db.close();
				bw1.close();bw2.close();bw3.close();bw4.close();bw5.close(); //bw6.close();
				bw1r.close();bw1r_f1.close();bw1r_f12.close();bw1r_c.close();
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
