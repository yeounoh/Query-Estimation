import java.awt.List;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;

// https://github.com/yeounoh/Query-Estimation.git
public class QueryEstimation {
	
	public class Configuration {
		String db_name;
		String tb_name;
		int data_type;
		int n_rep;
		
		//all data
		int[] s_size;
		
		//synthetic data experiment only
		int n_worker;
		int sampling_type;
		double lambda;
		
		public Configuration(String db_name, String tb_name, int data_type, int n_rep, int[] s_size){
			this.db_name = db_name;
			this.tb_name = tb_name;
			this.data_type = data_type;
			this.n_rep = n_rep;
			this.s_size = s_size; //total sample size
		}
		
		public void extraParam(int n_worker, int sampling_type, double lambda){
			this.n_worker = n_worker; //(s_size/n_worker) samples per worker
			this.sampling_type = sampling_type;
			this.lambda = lambda;
		}
	}
	
	public class Result {
		double[] cnt;
		double[] sum;
		double[] avg;
		double[] measure; 
		
		public Result(double[] cnt, double[] sum, double[] avg, double[] measure){
			this.cnt = cnt;
			this.sum = sum;
			this.avg = avg;
			this.measure = measure;
		}
		
		public String toString(){
			String str = "";
			for(double c : cnt)
				str += c + " ";
			for(double s : sum)
				str += s + " ";
			for(double a : avg)
				str += a + " ";
			for(double m : measure)
				str += m + " ";
			return str;
		}
	}
	
	/**
	 * 
	 * @param sample
	 * @param n_bkt
	 * @param bkt_type: 0- naive, 1- ER fixed, 2- ER auto
	 * @param est_type: 0- chao92-based, 1- f1-based, 2- f12-based
	 * @return
	 */
	public Result bucketApproach(Object[] sample, int n_bkt, double th, int bkt_type, int est_type){
		Bucket[] buckets; //number of buckets may vary
		double[] sum_by_bucket;
		double[] cnt_by_bucket; //number of samples
		double[] chao_by_bucket; //uniq items number estimation
		double[] sc_by_bucket;
		double[] cv_by_bucket;
		
		Estimator est = new Estimator(sample);
		if(bkt_type == 0){
			buckets = new Bucket[]{new Bucket(est.getMin(),est.getMax())};
		}
		else if(bkt_type == 1){
			buckets = new Bucket[n_bkt];
			double width = (est.getMax() - est.getMin())/n_bkt; 
			for(int bi=0;bi<n_bkt;bi++){
				buckets[bi] = new Bucket(bi*width, (bi+1)*width);
			}
		}
		else if(bkt_type == 2){
			buckets = est.autoBuckets(th, sample);
		}
		
		for(Object s : sample){
			if(s==null)
				continue;
			
			double value = ((DataItem) s).value();
			for(Bucket b : buckets){
				if(b.getLowerB() <= value && b.getUpperB() >= value){ //inclusive
					b.insertSample(s);
					break;
				}
			}
		}
		
		double sum_t = 0, cnt_t = 0, uniq_t = 0, chao_t = 0, avg_sc = 0, avg_cv = 0;
		for(int bi=0;bi<buckets.length;bi++){
			Object[] samples_b = buckets[bi].getSamples().toArray();
			est = new Estimator(samples_b);
			
			cnt_by_bucket[bi] = samples_b.length; 
			sum_by_bucket[bi] = est_type == 1 ? est.sum() : est.sumf1(); 
			sc_by_bucket[bi] = buckets[bi].getSampleCov();
			cv_by_bucket[bi] = buckets[bi].getCoeffVar();
			chao_by_bucket[bi] = est.chao92();
			
			cnt_t += cnt_by_bucket[bi];
			uniq_t += est.getUniqueCount();
			chao_t += chao_by_bucket[bi];
			sum_t += sum_by_bucket[bi];
			avg_sc += sc_by_bucket[bi] * (double) samples_b.length/sample.length; 
			avg_cv += cv_by_bucket[bi] * (double) samples_b.length/sample.length;
		}
		
		double[] cnt = {cnt_t, uniq_t, chao_t};
		double[] sum = {sum_t};
		double[] measure = {avg_sc, avg_cv};
		
		return new Result(cnt, sum, null, measure);
	}
	
	public void runExperiment(Configuration conf) throws Exception{
		boolean do_gen = true;
		int n_class = 0; // ground truth for C (# of species), used for random sample generation
		int p_size = 0;
		
		Database db = new DataGenerator().generateDataset(conf.db_name, conf.tb_name, do_gen, conf.data_type);
		
		//data statistics
		int[][] uniq_rep = new int[conf.s_size.length][conf.n_rep]; //unique data items
		
		//estimate population statistics from samples (method1: naive)
		double[][] naive_rep = new double[conf.s_size.length][conf.n_rep];
		double[][] naivef1_rep = new double[conf.s_size.length][conf.n_rep];
		double[][] estf12_rep = new double[conf.s_size.length][conf.n_rep];
		double[][] estsc_rep = new double[conf.s_size.length][conf.n_rep]; //sample coverage
		double[][] estcv_rep = new double[conf.s_size.length][conf.n_rep]; //coefficient of variance
		
		//estimate population statistics from samples (method2: bucket)
		double[][] est_bkt_rep = new double[conf.s_size.length][conf.n_rep];
		double[][] estf1_bkt_rep = new double[conf.s_size.length][conf.n_rep];
		double[][] estf12_bkt_rep = new double[conf.s_size.length][conf.n_rep];
		double[][] sc_bkt_rep = new double[conf.s_size.length][conf.n_rep];
		double[][] cv_bkt_rep = new double[conf.s_size.length][conf.n_rep];
		int[][] auto_b_size_rep = new int[conf.s_size.length][conf.n_rep];
		
		for(int ri=0;ri<conf.n_rep;ri++){
			for(int si=0;si<conf.s_size.length;si++){
				//data samples to run experiments
				Object[] sample;
				
				//synthetic data experiment
				if(conf.data_type == 1 || conf.data_type == 2){
					n_class = conf.data_type == 1 ? 100 : 50;
					
					ArrayList<Object> samples = new ArrayList<Object>();
					for(int i=0;i<conf.n_worker;i++){
						db.sampleByRandom((int) Math.ceil(conf.s_size[si]/conf.n_worker), conf.tb_name, n_class, conf.sampling_type,conf.lambda);
					}
					while(samples.size() > conf.s_size[si])
						samples.remove(samples.size()-1);
					sample = samples.toArray();
				}
				//real data experiment
				else if(conf.data_type == 3 || conf.data_type == 4){
					p_size = conf.data_type == 3 ? 499 : 327;
					
					sample = db.sampleByTime(conf.s_size[si], conf.tb_name);
				}
				
				//naive approach
				Result naive = bucketApproach(sample, 1, 0.0, 0, 0);
				Result naive_f1 = bucketApproach(sample, 1, 0.0, 0, 1);
				
				//ER fixed approach
				Result bkt = bucketApproach(sample, 5, 0.0, 1, 0);
				Result bkt_f1 = bucketApproach(sample, 5, 0.0, 1, 1);
				
				//ER auto approach
				Result bkt_auto = bucketApproach(sample, 0, 0.05, 2, 0);
				Result bkt_auto_f1 = bucketApproach(sample, 0, 0.05, 2, 1);
				
				//estimate population statistics from samples (method1: naive)
				est_rep[si][ri] = est.sum();
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
		}
		
	}
	
	public static void main(String[] args){
		try{
			//experimental parameters
			int data_type = 1; //1- uniform, 2- syntGDP, 3- realGDP, 4- solar
						
			if(amt_data){
				//database configuration-2
				String db_name = "gdp";
				String table_name = "amt";
				
				int p_size = 0;
				Database db = null;
				if(data_type == 3){
					p_size = 499; // number of samples (GDP)
					db = new DataGenerator().generateDataset(db_name, table_name, do_gen, data_type);
				}
				else if(data_type == 4){
					p_size = 327; // number of samples (Solar)
					db = new DataGenerator().generateDataset(db_name, table_name, do_gen, data_type);
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
