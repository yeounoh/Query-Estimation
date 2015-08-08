import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.math.*;
import org.apache.commons.math.optimization.fitting.WeightedObservedPoint;
import org.apache.commons.math3.analysis.MultivariateFunction;
import org.apache.commons.math3.analysis.interpolation.MicrosphereInterpolator;
import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;

// https://github.com/yeounoh/Query-Estimation.git
public class QueryEstimation {
	
	public static int[] incrementalSamples(int min, int max, int inc){
		int[] s_size = new int[(int) Math.ceil((max-min)/(double) inc)+1];
		for(int i=0;i<s_size.length-1;i++)
			s_size[i] = min + i*inc;
		s_size[s_size.length-1] = max;
		return s_size;
	}
	
	/**
	 * 
	 * @param sample
	 * @param n_bkt
	 * @param bkt_type: 1- ER fixed, 2- ER auto
	 * @param est_type: 0- chao92-based, 1- f1-based, 2- f12-based, 3- MC-based
	 * @return
	 */
	public Result bucketApproach(Object[] sample, int n_bkt, double th, int bkt_type, int est_type, boolean ub_test){
		Bucket[] buckets= null; //number of buckets may vary
		Estimator est = new Estimator(sample);
		
		if(bkt_type == 1){
			buckets = new Bucket[n_bkt];
			double width = (est.getMax() - est.getMin())/n_bkt; 
			for(int bi=0;bi<n_bkt;bi++){
				buckets[bi] = new Bucket(Math.floor(bi*width+est.getMin()), Math.ceil((bi+1)*width+est.getMin()));
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
					else if(n_bkt == 1 && bkt_type == 1)
						System.out.println("what?" + value + " bigger than " + b.getUpperB());
				} 
			}
		}
		else if(bkt_type == 2){
			buckets = est.autoBuckets(th, sample, ub_test);
		}
		
		double[] sum_by_bucket = new double[buckets.length];
		double[] sum_by_bucket_f1 = new double[buckets.length];
		double[] cnt_by_bucket = new double[buckets.length]; //number of samples
		double[] chao_by_bucket = new double[buckets.length]; //uniq items number estimation
		double[] sc_by_bucket = new double[buckets.length];
		double[] cv_by_bucket = new double[buckets.length];
		double[] csum_by_bucket = new double[buckets.length];
		
		double sum_t = 0, sumf1_t = 0,cnt_t = 0, uniq_t = 0, chao_t = 0, avg_sc = 0, avg_cv = 0, csum_t = 0;
		double avg_t = 0, avgf1_t = 0, cavg_t;
		
		for(int bi=0;bi<buckets.length;bi++){
			Object[] samples_b = buckets[bi].getSamples().toArray();
			est = new Estimator(samples_b);
			chao_by_bucket[bi] = est.chao92();
			chao_t += chao_by_bucket[bi];
		}
		
		for(int bi=0;bi<buckets.length;bi++){
			Object[] samples_b = buckets[bi].getSamples().toArray();
			est = new Estimator(samples_b);
			
			cnt_by_bucket[bi] = samples_b.length; 
			sum_by_bucket[bi] = ub_test? est.sumEstUpperBound():est.sumEst(); 
			sum_by_bucket_f1[bi] = est.sumf1(); //System.out.println("bkt"+(bi+1)+", diff:"+(est.sumEst()-est.sumf1())+", "+est.sumEst()+", "+est.sumf1());
			sc_by_bucket[bi] = buckets[bi].getSampleCov();
			cv_by_bucket[bi] = buckets[bi].getCoeffVar();
			//chao_by_bucket[bi] = est.chao92();
			csum_by_bucket[bi] = est.csum();
			
			cnt_t += cnt_by_bucket[bi];
			uniq_t += est.getUniqueCount();
			//chao_t += chao_by_bucket[bi];
			sum_t += sum_by_bucket[bi];
			sumf1_t += sum_by_bucket_f1[bi];
			csum_t += csum_by_bucket[bi];
			avg_sc += sc_by_bucket[bi] * (double) samples_b.length/sample.length; 
			avg_cv += cv_by_bucket[bi] * (double) samples_b.length/sample.length;
			
			avg_t += sum_by_bucket[bi]/chao_by_bucket[bi] * (chao_by_bucket[bi]/chao_t);
			avgf1_t += sum_by_bucket_f1[bi]/est.getF1Count() * (chao_by_bucket[bi]/chao_t);
		}
		//avg_t = sum_t/chao_t;
		//avgf1_t = sumf1_t/chao_t;
		cavg_t = csum_t/uniq_t;
		double max_orig = buckets[buckets.length-1].max();
		double unknown_cnt= (buckets[buckets.length-1].countEst() - buckets[buckets.length-1].getUnique());
		double sample_cov = buckets[buckets.length-1].getSampleCov();
		double max_est = (unknown_cnt==0 && sample_cov != 0)? max_orig : -1;
		double min_orig = buckets[0].min();
		unknown_cnt= (buckets[0].countEst() - buckets[0].getUnique());
		double min_est = (unknown_cnt==0 && sample_cov != 0)? min_orig : -1; 
		
		double[] cnt = {cnt_t, uniq_t, chao_t, buckets.length};
		double[] sum = {sum_t, csum_t};
		double[] measure = {avg_sc, avg_cv};
		double[] other = {avg_t, max_orig, max_est, cavg_t, min_orig, min_est,est.getSpecificItemCnt(),sumf1_t,avgf1_t}; 
		
		return new Result(cnt, sum, measure, other);
	}
	
	/**
	 * 
	 * @param sample
	 * @param n_bkt
	 * @param th CV threshold to how much data skew to tolerate per bucket
	 * @param n_worker: number of worker for monte carlo method
	 * @param est_type: 0- chao92-based, 1- f1-based, 2- f12-based, 3- MC-based
	 * @return
	 * @throws IOException 
	 */
//	public Result bucketMCApproach(Object[] sample, int[] n_w, double th,boolean ideal,boolean ub_test) throws IOException{
//		Bucket[] buckets= null; //number of buckets may vary
//		Estimator est = new Estimator(sample);
//		
//		int n = sample.length;
//		
//		buckets = est.autoBuckets(th, sample, ub_test);
//		
//		double[] sum_by_bucket = new double[buckets.length];
//		double[] sum_by_bucket_f1 = new double[buckets.length];
//		double[] cnt_by_bucket = new double[buckets.length]; //number of samples
//		double[] chao_by_bucket = new double[buckets.length]; //uniq items number estimation
//		double[] csum_by_bucket = new double[buckets.length];
//		
//		double sum_t = 0, sumf1_t = 0, cnt_t = 0, uniq_t = 0, chao_t = 0, avg_sc = 0, avg_cv = 0, csum_t = 0;
//		double avg_t = 0, avgf1_t = 0, cavg_t;
//		for(int bi=0;bi<buckets.length;bi++){
//			Object[] samples_b = buckets[bi].getSamples().toArray();
//			est = new Estimator(samples_b);
//			
//			int n_b = samples_b.length;
//			
//			cnt_by_bucket[bi] = samples_b.length; 
//			
//			int sample_left = n_b;
//			int[] n_w_b = new int[n_w.length];
//			for(int i=0;i<n_w.length;i++){
//				n_w_b[i] = (int) Math.min(Math.ceil((double) n_w[i] * ((double) n_b/n)),sample_left);
//				sample_left -= n_w_b[i];
//			}
//			if(sample_left > 0)
//				n_w_b[0] += sample_left;
//			
//			double[] mc = MonteCarloPolyFit(samples_b, n_w_b,ideal).summary(); //MonteCarloApproach(samples_b, n_w_b, est_type,ideal,false).summary();
//			sum_by_bucket[bi] =  mc[1];
//			sum_by_bucket_f1[bi] = mc[3];
//			chao_by_bucket[bi] = mc[0];
//			csum_by_bucket[bi] = est.csum();
//			
//			cnt_t += cnt_by_bucket[bi];
//			uniq_t += est.getUniqueCount();
//			chao_t += chao_by_bucket[bi];
//			sum_t += sum_by_bucket[bi];
//			sumf1_t += sum_by_bucket_f1[bi];
//			csum_t += csum_by_bucket[bi];
//		}
//		avg_t = sum_t/chao_t;
//		avgf1_t = sumf1_t/chao_t;
//		cavg_t = csum_t/uniq_t;
//		double[] cnt = {cnt_t, uniq_t, chao_t, buckets.length};
//		double[] sum = {sum_t, csum_t};
//		double[] measure = {avg_sc, avg_cv};
//		double[] other = {avg_t,cavg_t,sumf1_t,avgf1_t};
//		
//		return new Result(cnt, sum, measure, other);
//	}
		
	public Result MonteCarloPolyFit(Object[] sample, int[] n_w, boolean ideal) throws IOException {
		
		Estimator est = new Estimator(sample);
		
		int cnt_lb = est.getUniqueCount();
		int cnt_ub = (int) Math.ceil(est.chao92());
		if(est.chao92() == Double.POSITIVE_INFINITY)
			cnt_ub = cnt_lb;
		
		double[] lambda = {-4.0, -2.0, 0.0, 2.0, 4.0};
		//double[][] coeffs = new double[lambda.length][3];
		//PolynomialCurveFitter fitter = PolynomialCurveFitter.create(2);
		
		//maximum of 5 data points
		int jump = (int) Math.ceil((cnt_ub-cnt_lb)/5); 
		
		int i_last = jump == 0? 1 : (cnt_ub-cnt_lb+1)/jump; 
		double[][] X = new double[i_last*lambda.length][2]; //x_i = <C_i,lambda_i>
		double[] Y = new double[i_last*lambda.length];
		for(int j=0;j<lambda.length;j++){
			//WeightedObservedPoints obs = new WeightedObservedPoints();
			for(int i=0;i<i_last;i++){ 
				int C = cnt_lb + i*jump;
				X[i][0] = C;
				for(double l : lambda){
					X[i][1] = l;
					Y[i] = est.MonteCarlo(C, n_w, l, ideal); 
				}
				//obs.add(C,est.MonteCarlo(C, n_w, lambda[j], ideal)); 
			}
			//coeffs[j] = fitter.fit(obs.toList());
		}
		//System.out.println("coeff: "+coeff.length + ", ["+coeff[2]+","+coeff[1]+","+coeff[0]+"]");
		
		MicrosphereInterpolator interpolator = new MicrosphereInterpolator();
		MultivariateFunction f = interpolator.interpolate(X, Y);
		
		int best_cnt = cnt_lb;
		double error = Double.MAX_VALUE;
		double best_lambda = 0.0;
		for(int j=0;j<lambda.length;j++){
			for(int i=cnt_lb;i<=cnt_ub;i++){
				//double y = coeffs[j][2] * i * i + coeffs[j][1] *i + coeffs[j][0]; //coeff[2]:a, coeff[1]:b, coeff[0]:c
				double[] x = {i,lambda[j]};
				double y = f.value(x);
				if(y < error && Math.abs(error-y) > 0.01){
					error = y;
					best_cnt = i;
					best_lambda = lambda[j];
				}
			}
		}
		//System.out.println("n="+sample.length+", c="+cnt_lb+", Chao="+cnt_ub+", C^="+best_cnt+"(l="+best_lambda+")");
		double best_est = est.sumEst(best_cnt);
		double best_est_f1 = est.sumf1Est(best_cnt);
		
		double[] cnt = {best_cnt};
		double[] sum = {best_est};
		double[] measure = {};
		double[] other = {best_est/best_cnt, best_est_f1,best_est_f1/best_cnt}; //, best_lambda};
		
		return new Result(cnt, sum, measure, other);
	}
	
	public void runExperiment(Configuration conf) throws Exception{
		boolean do_gen = true;
		int n_class = 0; // ground truth for C (# of species), used for random sample generation
		String fname = "./result/";
		switch (conf.data_type) {
			case 1: fname += "uniform"; break;
			case 2: fname += "syntGDP"; break;
			case 3: fname += "realGDP"; break;
			case 4: fname += "employee"; break;
			case 5: fname += "evm"; break;
			case 6: fname += "evm_app"; break;
			case 7: fname += "VLDB"; break;
			case 8: fname += "uniform_w"+ conf.n_worker; break;
			case 9: fname += "uniform_l"+ conf.lambda; break;
			case 10: fname += "revenue"; break;
			case 11: fname += "uniform_w"+ conf.n_worker +"_l"+ conf.lambda; break;
			default: fname += "N/S.txt"; break;
		}
		if(conf.heatmap)
			fname += "_hm";
		else if(conf.st_only)
			fname += "_only";
		else if(conf.st_inject)
			fname += "_inject";
		else if(!conf.pub_val_corr)
			fname += "_unc";
		fname += ".txt";
		
		if(conf.data_type == 1 || conf.data_type == 2 || conf.data_type == 8 || conf.data_type == 9 || conf.data_type == 11){
			if(conf.data_type == 2)
				n_class = 50;
			else
				n_class = 100;
		}
		
		Database db = new DataGenerator().generateDataset(conf.db_name, conf.tb_name, do_gen, conf.data_type, conf.pub_val_corr);
		
		//method1: naive
		double[][][] naive_rep = (conf.mc_index || conf.heatmap)? null : new double[conf.s_size.length][conf.n_rep][]; 
		double[][][] naive_ub_rep = (conf.mc_index || conf.heatmap)? null : new double[conf.s_size.length][conf.n_rep][]; 
		
		//method2: bucket
		double[][][] bkt_auto_rep = conf.mc_index? null : new double[conf.s_size.length][conf.n_rep][];
		
		//method3: Monte-Carlo Simulation
		double[][][] mc_app_rep = conf.heatmap? null : new double[conf.s_size.length][conf.n_rep][];
		
		//method4: Monte-Carlo Bucket Simulation
		//double[][][] mcbkt_rep = (conf.mc_index || conf.heatmap)? null : new double[conf.s_size.length][conf.n_rep][];
		
		for(int ri=0;ri<conf.n_rep;ri++){
			if(ri%100 == 0)
				System.out.print("\n");
			System.out.print("."); //progress meter
			for(int si=0;si<conf.s_size.length;si++){ //System.out.print(si + "/" +conf.s_size.length + " ");
				//data samples to run experiments
				Object[] sample = null;
				int[] n_w = null;
				int assigned_sample = 0;
				
				//synthetic data experiment
				if(conf.data_type == 1 || conf.data_type == 2 || conf.data_type == 8 || conf.data_type == 9 || conf.data_type == 11){ 
					n_w = new int[conf.n_worker];
					ArrayList<Object> samples = new ArrayList<Object>();
					
					boolean streaker_only = conf.st_only; //default: false
					boolean inject_streaker = conf.st_inject; //default: false
					boolean injected = false;
					for(int i=0;i<conf.n_worker;i++){
						if(streaker_only){
							n_w[i] = Math.min(conf.s_size[si] - assigned_sample, n_class);
						}
						else{
							if(inject_streaker){
								
								if(conf.s_size[si] >= 160){
									if(!injected){
										n_w[i] = 100;
										injected = true;
									}
									else{
										n_w[i] = Math.min((int) Math.ceil((conf.s_size[si]-100)/(conf.n_worker-1)), n_class);
									}
								}
								else{
									n_w[i] = Math.min((int) Math.ceil(conf.s_size[si]/conf.n_worker), n_class);
								}
							}
							else{
								n_w[i] = Math.min((int) Math.ceil(conf.s_size[si]/conf.n_worker), n_class);
							}
						}
						
						if(n_w[i] == 0 && assigned_sample < conf.s_size[si]){
							n_w[i] = 1;
						}
						assigned_sample += n_w[i];
						
						//Sample randomly from the base_table
						Object[] s_worker = db.sampleByRandom(n_w[i], conf.tb_name, n_class, 
								conf.sampling_type, conf.lambda); 
						for(Object s : s_worker){
							samples.add(s); 
						}
					} 
					while(samples.size() > conf.s_size[si])
						samples.remove(samples.size()-1);
					sample = samples.toArray();
				}
				//real data experiment
				else if(conf.data_type == 3 || conf.data_type == 4 || conf.data_type == 5 || conf.data_type == 6 ||
						conf.data_type == 7 || conf.data_type == 10){ 
					if(conf.n_rep == 1) //single data set
						//load data in temporal order from a crowd_sourced table
						sample = db.sampleByTime(conf.s_size[si], conf.tb_name);
					else{ //multiple data sets
						db = new DataGenerator().generateDataset(conf.db_name, conf.tb_multi[ri], do_gen, conf.data_type, conf.pub_val_corr);
						sample = db.sampleByTime(conf.s_size[si], conf.tb_multi[ri]);
					}
						
					HashMap<String,Integer> map = new HashMap();
					for(Object s : sample){
						String id = ((DataItem) s).sourceID();
						if(!map.containsKey(id))
							map.put(id,new Integer(1));
						else
							map.replace(id, new Integer(map.get(id).intValue() + 1));
					}
					
					Set<String> keys = map.keySet();
					Iterator itr = keys.iterator();
					n_w = new int[keys.size()];
					int idx = 0;
					while(itr.hasNext()){
						n_w[idx++] = (map.get(itr.next())).intValue();
					}
				}
				//unknown data type
				else{
					System.out.println("Unknown data type");
					return;
				}
				long startTime = System.currentTimeMillis();
				//naive approach
				Result naive = (conf.mc_index || conf.heatmap)? null : bucketApproach(sample, 1, 0.0, 1, 0,false);
				//Result naive_ub = (conf.mc_index || conf.heatmap)? null : bucketApproach(sample, 1, 0.0, 1, 0,true);
				long endTime = System.currentTimeMillis();
				System.out.print("n="+conf.s_size[si] + " "+(endTime - startTime));
				startTime = System.currentTimeMillis();
				//ER auto approach
				Result bkt_auto = conf.mc_index? null : bucketApproach(sample, 0, 0.05, 2, 0,false);
				endTime = System.currentTimeMillis();
				System.out.print(" "+(endTime - startTime));
				startTime = System.currentTimeMillis();
				//Monte-Carlo Simulation
				boolean test = conf.mc_index? true : false;
				Result mc_app = conf.heatmap? null : MonteCarloPolyFit(sample, n_w,false); 
				//MonteCarloApproach(sample, n_w, 0, false,test);
				if(conf.mc_index) {
					System.out.println("mc_index test is outdated!"); 
					//MonteCarloApproach(sample, n_w, 0, true,test);
					continue;
				}
				endTime = System.currentTimeMillis();
				System.out.print(" "+(endTime - startTime)+"\n");
				//Monte-Carlo bucket simulation
				//Result mcbkt = (conf.mc_index || conf.heatmap)? null : bucketMCApproach(sample, n_w, 0.05, false);
				
				//estimate population statistics
				if(!conf.heatmap){
					naive_rep[si][ri] = naive.summary();
					mc_app_rep[si][ri] = mc_app.summary(); 
					//mcbkt_rep[si][ri] = mcbkt.summary();
				}
				bkt_auto_rep[si][ri] = bkt_auto.summary(); 
				//naive_ub_rep[si][ri] = naive_ub.summary();
			}
		}
		if(conf.mc_index)	return;
		
		//---------heat map for max bucket-------------//
		if(conf.heatmap){
			FileOutputStream fos_hm_orig= new FileOutputStream("./result/heatmap_max.txt");
			BufferedWriter bw_hm_orig= new BufferedWriter(new OutputStreamWriter(fos_hm_orig));
			FileOutputStream fos_hm_est= new FileOutputStream("./result/heatmap_min.txt");
			BufferedWriter bw_hm_est= new BufferedWriter(new OutputStreamWriter(fos_hm_est));
		
			int num_bin = 10, bin_width=10, max_value=1000, min_value=0;
			HashMap<String,Integer> freq_mat_max = new HashMap<String,Integer>();
			HashMap<String,Integer> freq_mat_min = new HashMap<String,Integer>();
			Bucket[] bins_max = new Bucket[num_bin];
			Bucket[] bins_min = new Bucket[num_bin];
			
			for(int si=0;si<conf.s_size.length;si++){
				bins_max = new Bucket[num_bin];
				bins_min = new Bucket[num_bin];
				for(int bi=0;bi<bins_max.length;bi++){
					bins_max[bi] = new Bucket(max_value-((bins_max.length-1-bi)+1)*bin_width,max_value-(bins_max.length-1-bi)*bin_width);
					bins_min[bi] = new Bucket(min_value+bi*bin_width,min_value+(bi+1)*bin_width);
				}
				
				for(int ri=0;ri<conf.n_rep;ri++){
					double max = bkt_auto_rep[si][ri][9]; //10:est, 9:obs
					double min = bkt_auto_rep[si][ri][12]; //13:est, 12:obs
					
					for(Bucket b:bins_max){
						if(b.getLowerB()<=max && b.getUpperB()>=max){
							b.insertSample(new DataItem(null,0,""+max,max,0));
							break;
						}
					}
					for(Bucket b:bins_min){
						if(b.getLowerB()<=min && b.getUpperB()>=min){
							b.insertSample(new DataItem(null,0,""+min,min,0));
							break;
						}
					}
					
				}
				for(int bi=0;bi<bins_max.length;bi++){
					String coord = ""+bi+","+si;
					if(!freq_mat_max.containsKey(coord)){
						freq_mat_max.put(coord, new Integer(bins_max[bi].getCount()));
					}
					else{
						System.out.println("no!");
						System.exit(1);
					}
					if(!freq_mat_min.containsKey(coord)){
						freq_mat_min.put(coord, new Integer(bins_min[bi].getCount()));
					}
					else{
						System.out.println("no!");
						System.exit(1);
					}
				}
				
			}
			for(int bi=num_bin-1;bi>=0;bi--){
				String row_max = "";
				String row_min = "";
				for(int si=0;si<conf.s_size.length;si++){
					String coord = ""+bi+","+si;
					if(freq_mat_max.containsKey(coord)){
						row_max += " " + freq_mat_max.get(coord).intValue();
					}
					else
						row_max += " " + 0;
					
					if(freq_mat_min.containsKey(coord)){
						row_min += " " + freq_mat_min.get(coord).intValue();
					}
					else
						row_min += " " + 0;
				}
				bw_hm_orig.write(row_max);
				bw_hm_orig.newLine();bw_hm_orig.flush();
				bw_hm_est.write(row_min);
				bw_hm_est.newLine();bw_hm_est.flush();
			}
			bw_hm_orig.close();
			bw_hm_est.close();
		}
		//---------heat map for max bucket-------------//
		
		FileOutputStream fos= new FileOutputStream(fname);
		BufferedWriter bw= new BufferedWriter(new OutputStreamWriter(fos));
		if(!conf.heatmap){
			bw.write("0:s_size | 1:naive_avg | 2:naive_std | 3:naivef1 | 4:naivef1_std | 5:bkt_avg | 6:bkt_std | 7:bktf1_avg | 8:bktf1_std | "
					+ "9:bktauto_avg | 10:bktauto_std | 11:bktf1auto_avg | 12:bktf1auto_std | 13:uniq_avg | 14:uniq_std | 15:nbktauto_avg | 16:nbktf1auto_avg |"
					+ "17:mc_avg | 18:mc_std | 19:chao_avg | 20:chao_std | 21:csum_avg | 22:csum_std | 23:naive_avg_avg | 24:naivef1_avg_avg |"
					+ "25:bkt_avg_avg | 26:bktf1_avg_avg | 27:bktauto_avg_avg | 28:bktf1auto_avg_avg | 29:mc_avg_avg | 30:max_orig_avg | 31:max_est_avg |"
					+ "32:naive_cavg_avg | 33:mcf1_avg | 34:mcf1_std | 35:mcf1_avg_avg | 36:min_orig_avg | 37:min_est_avg | 38:mcbucket_avg | 39:mcbucketf1_avg | "
					+ "40:mcbucket_avg_avg | 41:mcbucketf1_avg_avg | 42:mc_avg_ideal | 43:mcf1_avg_ideal | 44:mcbucket_avg_ideal | 45:mcbucketf1_avg_ideal | 46: specific_item_cnt |"
					+ "47:max_std | 48:max_est_std | 49:min_std | 50:min_est_std | 51:naive_ub_avg | 52:max_report | 53:min_report |"
					+ "54:max_prec | 55:max_rec | 56:min_prec | 57:min_rec | 58:max_prec_obs | 59:max_rec_obs | 60:min_prec_obs | 61:min_rec_obs |");
			bw.newLine();
			bw.flush();
		}
		
		for(int si=0;si<conf.s_size.length;si++){
			double avg_csum=0, std_csum=0, avg_cavg=0;
			double avg=0, avg_f1=0, avg_bkt=0, avg_bktf1=0, avg_bkt_auto=0, avg_bktf1_auto=0, avg_uniq=0, avg_chao=0, avg_mc_app=0, avg_mcf1_app=0, avg_mcbkt=0, avg_mcbktf1=0, avg_mc_ideal=0, avg_mcf1_ideal=0, avg_mcbkt_ideal=0, avg_mcbktf1_ideal=0;
			double std=0, std_f1=0, std_bkt=0, std_bktf1=0, std_bkt_auto=0, std_bktf1_auto=0, std_uniq=0, std_chao=0, std_mc_app=0, std_mcf1_app=0, std_max=0,std_max_est=0,std_min=0,std_min_est=0;
			double avg_nbktauto=0, avg_nbktf1auto=0, avg_max_orig=0, avg_max_est=0, avg_min_orig=0, avg_min_est=0, avg_naive_ub=0;
			
			double avg_spec_cnt=0, max_report=0, min_report=0;
			double max_tp=0, max_fp=0, max_tn=0, max_fn=0, max_tp_obs=0, max_fp_obs=0, max_tn_obs=0, max_fn_obs=0 ;
			double min_tp=0, min_fp=0, min_tn=0, min_fn=0, min_tp_obs=0, min_fp_obs=0, min_tn_obs=0, min_fn_obs=0;
			
			double avg_avg=0, avg_avg_f1=0, avg_avg_bkt=0, avg_avg_bktf1=0, avg_avg_bkt_auto=0,avg_avg_bktf1_auto=0,avg_avg_mc_app=0, avg_avg_mcf1_app=0, avg_avg_mcbkt=0, avg_avg_mcbktf1=0;
			for(int ri=0;ri<conf.n_rep;ri++){
				if(!conf.heatmap){
					avg_csum += naive_rep[si][ri][5]/conf.n_rep;
					avg_cavg += naive_rep[si][ri][11]/conf.n_rep;
					
					avg += naive_rep[si][ri][4]/conf.n_rep;
					avg_avg += naive_rep[si][ri][8]/conf.n_rep;
					avg_f1 += naive_rep[si][ri][15]/conf.n_rep;
					avg_avg_f1 += naive_rep[si][ri][16]/conf.n_rep;
					avg_spec_cnt += naive_rep[si][ri][14]/conf.n_rep;
					
					//avg_naive_ub += naive_ub_rep[si][ri][4]/conf.n_rep;
					
					avg_bkt_auto += bkt_auto_rep[si][ri][4]/conf.n_rep;
					avg_avg_bkt_auto += bkt_auto_rep[si][ri][8]/conf.n_rep;
					avg_bktf1_auto += bkt_auto_rep[si][ri][15]/conf.n_rep;
					avg_avg_bktf1_auto += bkt_auto_rep[si][ri][16]/conf.n_rep;
					
					avg_uniq += naive_rep[si][ri][1]/conf.n_rep;
					
					avg_nbktauto += bkt_auto_rep[si][ri][3]/conf.n_rep;
					avg_nbktf1auto += bkt_auto_rep[si][ri][3]/conf.n_rep;
					
					avg_chao += naive_rep[si][ri][2]/conf.n_rep;
					
					avg_mc_app += mc_app_rep[si][ri][1]/conf.n_rep; 
					avg_avg_mc_app += mc_app_rep[si][ri][2]/conf.n_rep;
					avg_mcf1_app += mc_app_rep[si][ri][3]/conf.n_rep;
					avg_avg_mcf1_app += mc_app_rep[si][ri][4]/conf.n_rep;
					
//					avg_mcbkt += mcbkt_rep[si][ri][4]/conf.n_rep;
//					avg_avg_mcbkt += mcbkt_rep[si][ri][8]/conf.n_rep;
//					avg_mcbktf1 += mcbkt_rep[si][ri][10]/conf.n_rep;
//					avg_avg_mcbktf1 += mcbkt_rep[si][ri][11]/conf.n_rep;
				}
				avg_max_orig += bkt_auto_rep[si][ri][9]/conf.n_rep;
				if(bkt_auto_rep[si][ri][10] != -1){
					avg_max_est += bkt_auto_rep[si][ri][10];
					max_report++;
					if(bkt_auto_rep[si][ri][10] == 1000){
						max_tp++;
					}
					else
						max_fp++;
				}
				else{
					if(bkt_auto_rep[si][ri][9] == 1000)
						max_fn++;
					else
						max_tn++;
				}
				if(bkt_auto_rep[si][ri][9] == 1000){
					max_tp_obs++;
				}
				else
					max_fp_obs++;
				
				avg_min_orig += bkt_auto_rep[si][ri][12]/conf.n_rep;
				if(bkt_auto_rep[si][ri][13] != -1){
					avg_min_est += bkt_auto_rep[si][ri][13];
					min_report++;
					if(bkt_auto_rep[si][ri][13] == 10){
						min_tp++;
					}
					else
						min_fp++;
				}
				else{
					if(bkt_auto_rep[si][ri][12] == 10)
						min_fn++;
					else
						min_tn++;
				}
				if(bkt_auto_rep[si][ri][12] == 10){
					min_tp_obs++;
				}
				else
					min_fp_obs++;
			}
			avg_max_est = avg_max_est/max_report;
			avg_min_est = avg_min_est/min_report;
			max_report = max_report;
			min_report = max_report;
			
			//precision & recall (MIN/MAX query)
//			double max_prec = 1-max_tn/(max_fp+max_tn);//(max_tp+max_fp)/(max_tn+max_fp);//max_tp/(max_tp+max_fp);
//			double max_rec = max_tp/(max_tp+max_fn);
//			double min_prec = 1-min_tn/(min_fp+min_tn);//(min_tp+min_fp)/(min_tn+min_fp);//min_tp/(min_tp+min_fp);
//			double min_rec = min_tp/(min_tp+min_fn);
//			double max_prec_obs = 1-max_tn_obs/(max_fp_obs+max_tn_obs);//(max_tp_obs+max_fp_obs)/(max_tn_obs+max_fp_obs);//max_tp_obs/(max_tp_obs+max_fp_obs);
//			double max_rec_obs = max_tp_obs/(max_tp_obs+max_fn_obs);
//			double min_prec_obs = 1- min_tn_obs/(min_fp_obs+min_tn_obs);//(min_tp_obs+min_fp_obs)/(min_tn_obs+min_fp_obs);//min_tp_obs/(min_tp_obs+min_fp_obs);
//			double min_rec_obs = min_tp_obs/(min_tp_obs+min_fn_obs);
			
			if(!conf.heatmap){
				for(int ri=0;ri<conf.n_rep;ri++){
					std_csum += (naive_rep[si][ri][5]-avg_csum)*(naive_rep[si][ri][5]-avg_csum);
					std += (naive_rep[si][ri][4]-avg)*(naive_rep[si][ri][4]-avg);
					std_f1 += (naive_rep[si][ri][15]-avg_f1)*(naive_rep[si][ri][15]-avg_f1);
					std_bkt_auto += (bkt_auto_rep[si][ri][4]-avg_bkt_auto)*(bkt_auto_rep[si][ri][4]-avg_bkt_auto);
					std_bktf1_auto += (bkt_auto_rep[si][ri][15]-avg_bktf1_auto)*(bkt_auto_rep[si][ri][15]-avg_bktf1_auto);
					std_uniq += (naive_rep[si][ri][1]-avg_uniq)*(naive_rep[si][ri][1]-avg_uniq);
					std_chao += (naive_rep[si][ri][2]-avg_chao)*(naive_rep[si][ri][2]-avg_chao);
					std_mc_app += (mc_app_rep[si][ri][1]-avg_mc_app)*(mc_app_rep[si][ri][1]-avg_mc_app);
					std_mcf1_app += (mc_app_rep[si][ri][3]-avg_mcf1_app)*(mc_app_rep[si][ri][3]-avg_mcf1_app);
					std_max += (bkt_auto_rep[si][ri][9]-avg_max_orig)*(bkt_auto_rep[si][ri][9]-avg_max_orig);
					std_max_est += (bkt_auto_rep[si][ri][10]-avg_max_est)*(bkt_auto_rep[si][ri][10]-avg_max_est);
					std_min += (bkt_auto_rep[si][ri][12]-avg_min_orig)*(bkt_auto_rep[si][ri][12]-avg_min_orig);
					std_min_est += (bkt_auto_rep[si][ri][13]-avg_min_est)*(bkt_auto_rep[si][ri][13]-avg_min_est);;
				}
				std_csum = Math.sqrt(std_csum/conf.n_rep);
				std = Math.sqrt(std/conf.n_rep); std_f1 = Math.sqrt(std_f1/conf.n_rep); 
	
				std_bkt_auto = Math.sqrt(std_bkt_auto/conf.n_rep); 
				std_bktf1_auto = Math.sqrt(std_bktf1_auto/conf.n_rep); std_uniq = Math.sqrt(std_uniq/conf.n_rep);
				std_chao = Math.sqrt(std_chao/conf.n_rep); std_mc_app = Math.sqrt(std_mc_app/conf.n_rep);
				std_mcf1_app = Math.sqrt(std_mcf1_app/conf.n_rep);
			}
			std_max = Math.sqrt(std_max/conf.n_rep); std_max_est = Math.sqrt(std_max_est/conf.n_rep);
			std_min = Math.sqrt(std_min/conf.n_rep); std_min_est = Math.sqrt(std_min_est/conf.n_rep);
			
			DecimalFormat df = new DecimalFormat("#.00");
			bw.write(conf.s_size[si] + " " + df.format(avg) + " " + df.format(std) + " " + df.format(avg_f1) 
					+ " " + df.format(std_f1)	+ " " + df.format(avg_bkt) + " " + df.format(std_bkt) 
					+ " " + df.format(avg_bktf1) + " " + df.format(std_bktf1) + " " + df.format(avg_bkt_auto) 
					+ " " + df.format(std_bkt_auto) + " " + df.format(avg_bktf1_auto)
					+ " " + df.format(std_bktf1_auto) + " " + df.format(avg_uniq) + " " + df.format(std_uniq) 
					+ " " + df.format(avg_nbktauto) + " " + df.format(avg_nbktf1auto)
					+ " " + df.format(avg_mc_app) + " " + df.format(std_mc_app)
					+ " " + df.format(avg_chao) + " " + df.format(std_chao)
					+ " " + df.format(avg_csum) + " " + df.format(std_csum)
					+ " " + df.format(avg_avg) + " " + df.format(avg_avg_f1) + " " + df.format(avg_avg_bkt) + " " +df.format(avg_avg_bktf1)
					+ " " + df.format(avg_avg_bkt_auto) + " " + df.format(avg_avg_bktf1_auto) + " " + df.format(avg_avg_mc_app)
					+ " " + df.format(avg_max_orig) + " " + df.format(avg_max_est) + " " + df.format(avg_cavg)
					+ " " + df.format(avg_mcf1_app) + " " + df.format(std_mcf1_app) + " " + df.format(avg_avg_mcf1_app)
					+ " " + df.format(avg_min_orig) + " " + df.format(avg_min_est)
					+ " " + df.format(avg_mcbkt) + " " + df.format(avg_mcbktf1)
					+ " " + df.format(avg_avg_mcbkt) + " " + df.format(avg_avg_mcbktf1)
					+ " " + df.format(avg_mc_ideal) + " " + df.format(avg_mcf1_ideal)
					+ " " + df.format(avg_mcbkt_ideal) + " " + df.format(avg_mcbktf1_ideal) + " " + df.format(avg_spec_cnt)
					+ " " + df.format(std_max) + " " + df.format(std_max_est) + " " + df.format(std_min) + " " + df.format(std_min_est)
					+ " " + df.format(avg_naive_ub) + " " + df.format(max_report) + " " + df.format(min_report)
//					+ " " + df.format(max_prec) + " " + df.format(max_rec) + " " + df.format(min_prec) + " " + df.format(min_rec)
//					+ " " + df.format(max_prec_obs) + " " + df.format(max_rec_obs) + " " + df.format(min_prec_obs) + " " + df.format(min_rec_obs)
					);
			bw.newLine();
			bw.flush();
		}
	}
	
	public static void main(String[] args){
		
		//run experiments
		QueryEstimation qe = new QueryEstimation();
		try {
			/** ------------- micro-benchmark ------------ */
			//experimental setup: 
			//Configuration(String db_name, String tb_name, int data_type, int n_rep, int[] s_size)
			Configuration config1;
			
			//streaker
//			int[] s_size1a = {80,100,120,140,160,180,200,220,240};
//			config1 = new Configuration("synt_db","unif",1,10,s_size1a);
//			config1.extraParam(20, 2, 1.0, true, false, false,false);
//			qe.runExperiment(config1);
//			config1 = new Configuration("synt_db","unif",1,10,s_size1a);
//			config1.extraParam(20, 2, 1.0, false, true, false,false);
//			qe.runExperiment(config1);
			
			//num of sources
//			int[] s_size1b = {80,90,100,110,120,130,140,150,160};
//			config1 = new Configuration("synt_db","unif",8,20,s_size1b);
//			config1.extraParam(1,2,1.0,false,false,false,false);
//			qe.runExperiment(config1); //uniform data
//			config1.extraParam(20,2,1.0,false,false,false,false);
//			qe.runExperiment(config1); //uniform data
//			config1.extraParam(40,2,1.0,false,false,false,false);
//			qe.runExperiment(config1); //uniform data
//			config1.extraParam(3,2,1.0,false,false,false,false);
//			qe.runExperiment(config1); //uniform data
//		
//			//varying degrees of data skew
//			int[] s_size1c = {80,140,200,260,320,380,440,500};
//			config1 = new Configuration("synt_db","unif",9,20,s_size1c);
////			config1.extraParam(20,2,0.0,false,false,false,false); //1
////			qe.runExperiment(config1);
//			config1.extraParam(20,2,1.0,false,false,false,false); //0.6
//			qe.runExperiment(config1);
//			config1.extraParam(20,2,1.0,false,false,false,false); //0.13
//			config1.setPublicityValueCorr(false);
//			qe.runExperiment(config1);
//			config1.setPublicityValueCorr(true);
//			config1.extraParam(20,2,4.0,false,false,false,false); //0.018
//			qe.runExperiment(config1);
			
			// num of sources, skew (data_type 11)
//			int[] s_size1bc = {80,140,200,260,320,380,440,500};
//			config1 = new Configuration("synt_db","unif",11,20,s_size1bc);
//			config1.extraParam(5,2,0.0,false,false,false,false);
//			config1.setPublicityValueCorr(false);
//			qe.runExperiment(config1);
//			config1.extraParam(10,2,0.0,false,false,false,false);
//			config1.setPublicityValueCorr(false);
//			qe.runExperiment(config1);
//			config1.extraParam(100,2,0.0,false,false,false,false);
//			config1.setPublicityValueCorr(false);
//			qe.runExperiment(config1);
//			config1.extraParam(5,2,4.0,false,false,false,false);
//			config1.setPublicityValueCorr(false);
//			qe.runExperiment(config1);
//			config1.extraParam(10,2,4.0,false,false,false,false);
//			config1.setPublicityValueCorr(false);
//			qe.runExperiment(config1);
//			config1.extraParam(100,2,4.0,false,false,false,false);
//			config1.setPublicityValueCorr(false);
//			qe.runExperiment(config1);
//			config1.extraParam(5,2,4.0,false,false,false,false);
//			config1.setPublicityValueCorr(true);
//			qe.runExperiment(config1);
//			config1.extraParam(10,2,4.0,false,false,false,false);
//			config1.setPublicityValueCorr(true);
//			qe.runExperiment(config1);
//			config1.extraParam(100,2,4.0,false,false,false,false);
//			config1.setPublicityValueCorr(true);
//			qe.runExperiment(config1);
			
			//heatmap
//			int[] s_size1d = incrementalSamples(100,1000,100);
//			config1 = new Configuration("synt_db","unif",1,200,s_size1d);
//			config1.extraParam(20,2,1.0,false,false,true,false); //0.6 -> 0.3
//			qe.runExperiment(config1);
			
			//Indexing (Monte-Carlo)
//			int[] s_size1e = incrementalSamples(40,160,40);
//			config1 = new Configuration("synt_db","unif",1,1,s_size1e);
//			config1.extraParam(20,2,0.5,false,false,false,true); //0.6 -> 0.3
//			qe.runExperiment(config1);
			
			//regular synthetic data exp
			//int[] s_size1c = {200,300,400,500,600,700,800,900,1000,1100,1200,1300,1400,1500}; //upper bound
			int[] s_size1c = {100,200,300,400,500,600,700,800};
			config1 = new Configuration("synt_db","unif",1,20,s_size1c);
			config1.extraParam(20,2,1.0,false,false,false,false); //0.6
			qe.runExperiment(config1);
			
			/** -------------- real-benchmark ------------ */
			//real gdp data
//			int[] s_size3 = incrementalSamples(20,160,5); //496
//			Configuration config3 = new Configuration("real_db","gdp",3,1,s_size3); //data_type: 3, num_rep: 4
//			//config3.extraParam(new String[]{"gdp","gdp1","gdp2","gdp3"}); //need num_rep = 3
//			qe.runExperiment(config3); 
//			
//			//real employee data
//			int[] s_size4 = incrementalSamples(20,496,20); //989, 995
//			Configuration config4 = new Configuration("real_db","employee",4,1,s_size4);
//			//config4.extraParam(new String[]{"employee","employee1"});
//			qe.runExperiment(config4); 
//			
//			//real EVM data (photon beam)
//			int[] s_size5 = incrementalSamples(20,812,20);
//			Configuration config5 = new Configuration("real_db","evm",5,1,s_size5);
//			qe.runExperiment(config5); 
			
			//real EVM data (Appendicitis)
//			int[] s_size6 = incrementalSamples(40,974,40);
//			Configuration config6 = new Configuration("real_db","evm",6,1,s_size6);
//			qe.runExperiment(config6); 
			
			 //real SIGMOD/VLDB
//			int[] s_size7 = incrementalSamples(20,498,20);
//			Configuration config7 = new Configuration("real_db","vldb",7,1,s_size7);
//			qe.runExperiment(config7);
			
			//real revenue data
//			int[] s_size10 = incrementalSamples(20,495,20);
//			Configuration config10 = new Configuration("real_db","revenue",10,1,s_size10);
//			//config10.extraParam(new String[]{"revenue","revenu1"}); //need num_rep = 2
//			qe.runExperiment(config10); 
			
			
		} 
		catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} 
	}
}
