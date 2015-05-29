import java.awt.List;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

// https://github.com/yeounoh/Query-Estimation.git
public class QueryEstimation {
	
	public static int[] incrementalSamples(int min, int max, int inc){
		int[] s_size = new int[(max-min)/inc];
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
	public Result bucketApproach(Object[] sample, int n_bkt, double th, int bkt_type, int est_type){
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
			buckets = est.autoBuckets(th, sample);
		}
		
		double[] sum_by_bucket = new double[buckets.length];
		double[] cnt_by_bucket = new double[buckets.length]; //number of samples
		double[] chao_by_bucket = new double[buckets.length]; //uniq items number estimation
		double[] sc_by_bucket = new double[buckets.length];
		double[] cv_by_bucket = new double[buckets.length];
		double[] csum_by_bucket = new double[buckets.length];
		
		double sum_t = 0, cnt_t = 0, uniq_t = 0, chao_t = 0, avg_sc = 0, avg_cv = 0, csum_t = 0;
		double avg_t = 0, cavg_t;
		for(int bi=0;bi<buckets.length;bi++){
			Object[] samples_b = buckets[bi].getSamples().toArray();
			est = new Estimator(samples_b);
			
			cnt_by_bucket[bi] = samples_b.length; 
			sum_by_bucket[bi] = est_type == 1 ? est.sumEst() : est.sumf1(); 
			sc_by_bucket[bi] = buckets[bi].getSampleCov();
			cv_by_bucket[bi] = buckets[bi].getCoeffVar();
			chao_by_bucket[bi] = est.chao92();
			csum_by_bucket[bi] = est.csum();
			
			cnt_t += cnt_by_bucket[bi];
			uniq_t += est.getUniqueCount();
			chao_t += chao_by_bucket[bi];
			sum_t += sum_by_bucket[bi];
			csum_t += csum_by_bucket[bi];
			avg_sc += sc_by_bucket[bi] * (double) samples_b.length/sample.length; 
			avg_cv += cv_by_bucket[bi] * (double) samples_b.length/sample.length;
		}
		avg_t = sum_t/chao_t;
		cavg_t = csum_t/uniq_t;
		double max_orig = est.getMax();
		double unknown_cnt= (buckets[buckets.length-1].countEst() - buckets[buckets.length-1].getUnique());
		double sample_cov = buckets[buckets.length-1].getSampleCov();
		double max_est = (unknown_cnt==0 && sample_cov != 0)? max_orig : -1;
		double min_orig = est.getMin();
		unknown_cnt= (buckets[0].countEst() - buckets[0].getUnique());
		double min_est = (unknown_cnt==0 && sample_cov != 0)? min_orig : -1;
		double[] cnt = {cnt_t, uniq_t, chao_t, buckets.length};
		double[] sum = {sum_t, csum_t};
		double[] measure = {avg_sc, avg_cv};
		double[] other = {avg_t, max_orig, max_est, cavg_t, min_orig, min_est,est.getSpecificItemCnt()};
		
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
	public Result bucketMCApproach(Object[] sample, int[] n_w, double th, int est_type,boolean ideal) throws IOException{
		Bucket[] buckets= null; //number of buckets may vary
		Estimator est = new Estimator(sample);
		
		int n = sample.length;
		
		buckets = est.autoBuckets(th, sample);
		
		double[] sum_by_bucket = new double[buckets.length];
		double[] cnt_by_bucket = new double[buckets.length]; //number of samples
		double[] chao_by_bucket = new double[buckets.length]; //uniq items number estimation
		double[] csum_by_bucket = new double[buckets.length];
		
		double sum_t = 0, cnt_t = 0, uniq_t = 0, chao_t = 0, avg_sc = 0, avg_cv = 0, csum_t = 0;
		double avg_t = 0, cavg_t;
		for(int bi=0;bi<buckets.length;bi++){
			Object[] samples_b = buckets[bi].getSamples().toArray();
			est = new Estimator(samples_b);
			
			int n_b = samples_b.length;
			
			cnt_by_bucket[bi] = samples_b.length; 
			
			int sample_left = n_b;
			int[] n_w_b = new int[n_w.length];
			for(int i=0;i<n_w.length;i++){
				n_w_b[i] = (int) Math.min(Math.ceil((double) n_w[i] * ((double) n_b/n)),sample_left);
				sample_left -= n_w_b[i];
			}
			if(sample_left > 0)
				n_w_b[0] += sample_left;
			
			double[] mc = MonteCarloApproach(samples_b, n_w_b, est_type,ideal).summary();
			sum_by_bucket[bi] =  mc[1];
			chao_by_bucket[bi] = mc[0];
			csum_by_bucket[bi] = est.csum();
			
			cnt_t += cnt_by_bucket[bi];
			uniq_t += est.getUniqueCount();
			chao_t += chao_by_bucket[bi];
			sum_t += sum_by_bucket[bi];
			csum_t += csum_by_bucket[bi];
		}
		avg_t = sum_t/chao_t;
		cavg_t = csum_t/uniq_t;
		double[] cnt = {cnt_t, uniq_t, chao_t, buckets.length};
		double[] sum = {sum_t, csum_t};
		double[] measure = {avg_sc, avg_cv};
		double[] other = {avg_t,cavg_t};
		
		return new Result(cnt, sum, measure, other);
	}
	
	public Result MonteCarloApproach(Object[] sample, int[] n_w, int est_type, boolean ideal) throws IOException {
		Estimator est = new Estimator(sample);
		double best_est = 0, best_cnt= 0;
		
		double error = Double.MAX_VALUE;
		int width = (int) est.chao92()/5;
		int cnt_lb = Math.max(est.getUniqueCount(), (int) Math.floor(est.chao92() - width));
		int cnt_ub = (int) Math.ceil(est.chao92() + width);
		for(int i=cnt_lb;i<=cnt_ub;i++){
			double error_b = Double.MAX_VALUE;
			double new_error = est.MonteCarlo(i, n_w, ideal);
			if(error_b < new_error)
				break;
			else
				error_b = new_error;
			
			double new_est = est_type == 0? est.sumEst(i) : est.sumf1Est(i);
			
			if(error > new_error){
				best_cnt = i;
				error = new_error;
				best_est = new_est;
				//System.out.println(error + "@(" + i + ")");
			}
		}
		
		double[] cnt = {best_cnt};
		double[] sum = {best_est};
		double[] measure = {};
		double[] other = {best_est/best_cnt}; //, best_lambda};
		
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
			default: fname += "N/S.txt"; break;
		}
		if(conf.heatmap)
			fname += "_hm";
		else if(conf.st_only)
			fname += "_only";
		else if(conf.st_inject)
			fname += "_inject";
		fname += ".txt";
		
		if(conf.data_type == 1 || conf.data_type == 2 || conf.data_type == 8 || conf.data_type == 9){
			if(conf.data_type == 2)
				n_class = 50;
			else
				n_class = 100;
		}
		
		Database db = new DataGenerator().generateDataset(conf.db_name, conf.tb_name, do_gen, conf.data_type);
		
		//method1: naive
		double[][][] naive_rep = conf.heatmap? null : new double[conf.s_size.length][conf.n_rep][]; 
		double[][][] naivef1_rep = conf.heatmap? null : new double[conf.s_size.length][conf.n_rep][];
		
		//method2: bucket
		//double[][][] bkt_rep = new double[conf.s_size.length][conf.n_rep][]; 
		//double[][][] bktf1_rep = new double[conf.s_size.length][conf.n_rep][];
		double[][][] bkt_auto_rep = new double[conf.s_size.length][conf.n_rep][]; 
		double[][][] bktf1_auto_rep = new double[conf.s_size.length][conf.n_rep][];
		
		//method3: Monte-Carlo Simulation
		double[][][] mc_app_rep = conf.heatmap? null : new double[conf.s_size.length][conf.n_rep][];
		double[][][] mcf1_app_rep = conf.heatmap? null : new double[conf.s_size.length][conf.n_rep][];
		//double[][][] mc_app_ideal_rep = new double[conf.s_size.length][conf.n_rep][];
		//double[][][] mcf1_app_ideal_rep = new double[conf.s_size.length][conf.n_rep][];
		
		//method4: Monte-Carlo Bucket Simulation
		double[][][] mcbkt_rep = conf.heatmap? null : new double[conf.s_size.length][conf.n_rep][];
		double[][][] mcbktf1_rep = conf.heatmap? null : new double[conf.s_size.length][conf.n_rep][];
		//double[][][] mcbkt_ideal_rep = new double[conf.s_size.length][conf.n_rep][];
		//double[][][] mcbktf1_ideal_rep = new double[conf.s_size.length][conf.n_rep][];
		
		for(int ri=0;ri<conf.n_rep;ri++){
			if(ri%100 == 0)
				System.out.print("\n");
			System.out.print("."); //progress meter
			for(int si=0;si<conf.s_size.length;si++){ //System.out.println(si + "/" +conf.s_size.length);
				//data samples to run experiments
				Object[] sample = null;
				int[] n_w = null;
				int assigned_sample = 0;
				
				//synthetic data experiment
				if(conf.data_type == 1 || conf.data_type == 2 || conf.data_type == 8 || conf.data_type == 9){ 
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
				else{ // data_type = 3, 4, 5, 6
					sample = db.sampleByTime(conf.s_size[si], conf.tb_name);
					
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
				
				//naive approach
				Result naive = conf.heatmap? null : bucketApproach(sample, 1, 0.0, 1, 0);
				Result naive_f1 = conf.heatmap? null : bucketApproach(sample, 1, 0.0, 1, 1);
				
				//ER fixed approach
				//Result bkt = bucketApproach(sample, 5, 0.0, 1, 0);
				//Result bktf1 = bucketApproach(sample, 5, 0.0, 1, 1);
				
				//ER auto approach
				Result bkt_auto = bucketApproach(sample, 0, 0.05, 2, 0);
				Result bktf1_auto = bucketApproach(sample, 0, 0.05, 2, 1);
				
				//Monte-Carlo Simulation
				int n_itr = 100;
				Result mc_app = conf.heatmap? null : MonteCarloApproach(sample, n_w, 0, false);
				Result mcf1_app = conf.heatmap? null : MonteCarloApproach(sample, n_w, 1, false);
				//Result mc_app = MonteCarloApproach(sample, n_w, n_itr, 0); //with data skew (Gradient descent)
				//Result mcf1_app = MonteCarloApproach(sample, n_w, n_itr, 1); //with data skew (Gradient descent)
				//Result mc_app_ideal = MonteCarloApproach(sample, n_w, 0, true); //ideal indexing
				//Result mcf1_app_ideal = MonteCarloApproach(sample, n_w, 1, true); //ideal indexing
				
				//Monte-Carlo bucket simulation
				Result mcbkt = conf.heatmap? null : bucketMCApproach(sample, n_w, 0.05, 0, false);
				Result mcbktf1 = conf.heatmap? null : bucketMCApproach(sample, n_w, 0.05, 1, false);
				//Result mcbkt_ideal = bucketMCApproach(sample, n_w, 0.05, 0, true);
				//Result mcbktf1_ideal = bucketMCApproach(sample, n_w, 0.05, 1, true);
				
				//estimate population statistics
				if(!conf.heatmap){
					naive_rep[si][ri] = naive.summary();
					naivef1_rep[si][ri] = naive_f1.summary();
					//bkt_rep[si][ri] = bkt.summary(); 
					//bktf1_rep[si][ri] = bktf1.summary();
					mc_app_rep[si][ri] = mc_app.summary(); 
					mcf1_app_rep[si][ri] = mcf1_app.summary(); 
					//mc_app_ideal_rep[si][ri] = mc_app_ideal.summary(); 
					//mcf1_app_ideal_rep[si][ri] = mcf1_app_ideal.summary(); 
					mcbkt_rep[si][ri] = mcbkt.summary();
					mcbktf1_rep[si][ri] = mcbktf1.summary();
					//mcbkt_ideal_rep[si][ri] = mcbkt_ideal.summary();
					//mcbktf1_ideal_rep[si][ri] = mcbktf1_ideal.summary();
				}
				bkt_auto_rep[si][ri] = bkt_auto.summary(); 
				bktf1_auto_rep[si][ri] = bktf1_auto.summary();
			}
		}
	
		//---------heat map for max bucket-------------//
		if(conf.heatmap){
			FileOutputStream fos_hm_orig= new FileOutputStream("./result/heatmap_max.txt");
			BufferedWriter bw_hm_orig= new BufferedWriter(new OutputStreamWriter(fos_hm_orig));
			FileOutputStream fos_hm_est= new FileOutputStream("./result/heatmap_min.txt");
			BufferedWriter bw_hm_est= new BufferedWriter(new OutputStreamWriter(fos_hm_est));
		
			int num_bin = 5;
			HashMap<String,Integer> freq_mat_max = new HashMap<String,Integer>();
			HashMap<String,Integer> freq_mat_min = new HashMap<String,Integer>();
			Bucket[] bins_max = new Bucket[num_bin];
			Bucket[] bins_min = new Bucket[num_bin];
			
			double init = 0;
			for(int bi=0;bi<bins_max.length;bi++){
				bins_max[bi] = new Bucket(init,init+100/num_bin);
				bins_min[bi] = new Bucket(init,init+100/num_bin);
				init+=100/num_bin;
			}
			for(int si=0;si<conf.s_size.length;si++){
				bins_max = new Bucket[num_bin];
				bins_min = new Bucket[num_bin];
				init = 0;
				for(int bi=0;bi<bins_max.length;bi++){
					bins_max[bi] = new Bucket(init,init+100/num_bin);
					bins_min[bi] = new Bucket(init,init+100/num_bin);
					init+=100/num_bin;
				}
				
				for(int ri=0;ri<conf.n_rep;ri++){
					double max = bkt_auto_rep[si][ri][10];
					double min = bkt_auto_rep[si][ri][13];
					
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
					+ "40:mcbucket_avg_avg | 41:mcbucketf1_avg_avg | 42:mc_avg_ideal | 43:mcf1_avg_ideal | 44:mcbucket_avg_ideal | 45:mcbucketf1_avg_ideal | 46: specific_item_cnt"
					+ "47:max_std | 48:max_est_std | 49:min_std | 50:min_est_std");
			bw.newLine();
			bw.flush();
		}
		
		for(int si=0;si<conf.s_size.length;si++){
			double avg_csum=0, std_csum=0, avg_cavg=0;
			double avg=0, avg_f1=0, avg_bkt=0, avg_bktf1=0, avg_bkt_auto=0, avg_bktf1_auto=0, avg_uniq=0, avg_chao=0, avg_mc_app=0, avg_mcf1_app=0, avg_mcbkt=0, avg_mcbktf1=0, avg_mc_ideal=0, avg_mcf1_ideal=0, avg_mcbkt_ideal=0, avg_mcbktf1_ideal=0;
			double std=0, std_f1=0, std_bkt=0, std_bktf1=0, std_bkt_auto=0, std_bktf1_auto=0, std_uniq=0, std_chao=0, std_mc_app=0, std_mcf1_app=0, std_max=0,std_max_est=0,std_min=0,std_min_est=0;
			double avg_nbktauto=0, avg_nbktf1auto=0, avg_max_orig=0, avg_max_est=0, avg_min_orig=0, avg_min_est=0;
			
			double avg_spec_cnt=0;
			
			double avg_avg=0, avg_avg_f1=0, avg_avg_bkt=0, avg_avg_bktf1=0, avg_avg_bkt_auto=0,avg_avg_bktf1_auto=0,avg_avg_mc_app=0, avg_avg_mcf1_app=0, avg_avg_mcbkt=0, avg_avg_mcbktf1=0;
			for(int ri=0;ri<conf.n_rep;ri++){
				if(!conf.heatmap){
					avg_csum += naive_rep[si][ri][5]/conf.n_rep;
					avg_cavg += naive_rep[si][ri][11]/conf.n_rep;
					
					avg += naive_rep[si][ri][4]/conf.n_rep;
					avg_avg += naive_rep[si][ri][8]/conf.n_rep;
					avg_f1 += naivef1_rep[si][ri][4]/conf.n_rep;
					avg_avg_f1 += naivef1_rep[si][ri][8]/conf.n_rep;
					avg_spec_cnt += naive_rep[si][ri][14]/conf.n_rep;
					
					//avg_bkt += bkt_rep[si][ri][4]/conf.n_rep;
					//avg_avg_bkt += bkt_rep[si][ri][8]/conf.n_rep;
					//avg_bktf1 += bktf1_rep[si][ri][4]/conf.n_rep;
					//avg_avg_bktf1 += bktf1_rep[si][ri][8]/conf.n_rep;
					
					avg_bkt_auto += bkt_auto_rep[si][ri][4]/conf.n_rep;
					avg_avg_bkt_auto += bkt_auto_rep[si][ri][8]/conf.n_rep;
					avg_bktf1_auto += bktf1_auto_rep[si][ri][4]/conf.n_rep;
					avg_avg_bktf1_auto += bktf1_auto_rep[si][ri][8]/conf.n_rep;
					
					avg_uniq += naive_rep[si][ri][1]/conf.n_rep;
					
					avg_nbktauto += bkt_auto_rep[si][ri][3]/conf.n_rep;
					avg_nbktf1auto += bktf1_auto_rep[si][ri][3]/conf.n_rep;
					
					avg_chao += naive_rep[si][ri][2]/conf.n_rep;
					
					avg_mc_app += mc_app_rep[si][ri][1]/conf.n_rep;
					avg_mcf1_app += mcf1_app_rep[si][ri][1]/conf.n_rep;
					avg_avg_mc_app += mc_app_rep[si][ri][2]/conf.n_rep;
					avg_avg_mcf1_app += mcf1_app_rep[si][ri][2]/conf.n_rep;
					//avg_mc_ideal += mc_app_rep[si][ri][1]/conf.n_rep;
					//avg_mcf1_ideal += mcf1_app_rep[si][ri][1]/conf.n_rep;
					
					avg_mcbkt += mcbkt_rep[si][ri][4]/conf.n_rep;
					avg_mcbktf1 += mcbktf1_rep[si][ri][8]/conf.n_rep;
					//avg_mcbkt_ideal += mcbkt_ideal_rep[si][ri][4]/conf.n_rep;
					//avg_mcbktf1_ideal += mcbktf1_ideal_rep[si][ri][8]/conf.n_rep;
					avg_avg_mcbkt += mcbkt_rep[si][ri][4]/conf.n_rep;
					avg_avg_mcbktf1 += mcbktf1_rep[si][ri][8]/conf.n_rep;
				}
				
				avg_max_orig += bkt_auto_rep[si][ri][9]/conf.n_rep;
				avg_max_est += bkt_auto_rep[si][ri][10]/conf.n_rep;
				avg_min_orig += bkt_auto_rep[si][ri][12]/conf.n_rep;
				avg_min_est += bkt_auto_rep[si][ri][13]/conf.n_rep;
			}
			
			if(!conf.heatmap){
				for(int ri=0;ri<conf.n_rep;ri++){
					std_csum += (naive_rep[si][ri][5]-avg_csum)*(naive_rep[si][ri][5]-avg_csum);
					std += (naive_rep[si][ri][4]-avg)*(naive_rep[si][ri][4]-avg);
					std_f1 += (naivef1_rep[si][ri][4]-avg_f1)*(naivef1_rep[si][ri][4]-avg_f1);
					//std_bkt += (bkt_rep[si][ri][4]-avg_bkt)*(bkt_rep[si][ri][4]-avg_bkt);
					//std_bktf1 += (bktf1_rep[si][ri][4]-avg_bktf1)*(bktf1_rep[si][ri][4]-avg_bktf1);
					std_bkt_auto += (bkt_auto_rep[si][ri][4]-avg_bkt_auto)*(bkt_auto_rep[si][ri][4]-avg_bkt_auto);
					std_bktf1_auto += (bktf1_auto_rep[si][ri][4]-avg_bktf1_auto)*(bktf1_auto_rep[si][ri][4]-avg_bktf1_auto);
					std_uniq += (naive_rep[si][ri][1]-avg_uniq)*(naive_rep[si][ri][1]-avg_uniq);
					std_chao += (naive_rep[si][ri][2]-avg_chao)*(naive_rep[si][ri][2]-avg_chao);
					std_mc_app += (mc_app_rep[si][ri][0]-avg_mc_app)*(mc_app_rep[si][ri][0]-avg_mc_app);
					std_mcf1_app += (mcf1_app_rep[si][ri][0]-avg_mcf1_app)*(mcf1_app_rep[si][ri][0]-avg_mcf1_app);
					std_max += (bkt_auto_rep[si][ri][9]-avg_max_orig)*(bkt_auto_rep[si][ri][9]-avg_max_orig);
					std_max_est += (bkt_auto_rep[si][ri][10]-avg_max_est)*(bkt_auto_rep[si][ri][10]-avg_max_est);
					std_min += (bkt_auto_rep[si][ri][12]-avg_min_orig)*(bkt_auto_rep[si][ri][12]-avg_min_orig);
					std_min_est += (bkt_auto_rep[si][ri][13]-avg_min_est)*(bkt_auto_rep[si][ri][13]-avg_min_est);;
				}
				std_csum = Math.sqrt(std_csum/conf.n_rep);
				std = Math.sqrt(std/conf.n_rep); std_f1 = Math.sqrt(std_f1/conf.n_rep); 
				//std_bkt = Math.sqrt(std_bkt/conf.n_rep);
				//std_bktf1 = Math.sqrt(std_bktf1/conf.n_rep); 
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
					+ " " + df.format(std_max) + " " + df.format(std_max_est) + " " + df.format(std_min) + " " + df.format(std_min_est));
			bw.newLine();
			bw.flush();
		}
	}
	
	public static void main(String[] args){
		//experimental setup: Configuration(String db_name, String tb_name, int data_type, int n_rep, int[] s_size)
		Configuration config1;
		
//		int[] s_size2 = {20,40,60,80,100,120,140,160,180,200,220,240,260,280,300,320,340,360,380,400,420,440,460,480,500};
//		Configuration config2 = new Configuration("synt_db","gdp",2,50,s_size2);
//		config2.extraParam(20, 2, 0.5, false, false);
		
		int[] s_size3 = incrementalSamples(20,496,20); //496
		int[] s_size4 = incrementalSamples(20,500,20);
		int[] s_size5 = incrementalSamples(20,812,20);
		int[] s_size6 = incrementalSamples(40,6160,40);
		int[] s_size7 = incrementalSamples(20,498,20);
		
		Configuration config3 = new Configuration("real_db","gdp",3,1,s_size3);
		Configuration config4 = new Configuration("real_db","employee",4,1,s_size4);
		Configuration config5 = new Configuration("real_db","evm",5,1,s_size5);
		Configuration config6 = new Configuration("real_db","evm",6,1,s_size6);
		Configuration config7 = new Configuration("real_db","vldb",7,1,s_size7);
		
		//run experiments
		QueryEstimation qe = new QueryEstimation();
		try {
			/** ------------- micro-benchmark ------------ */
			//streaker
//			int[] s_size1 = {20,40,60,80,100,120,140,160,180,200,220,240,260,280};
//			config1 = new Configuration("synt_db","unif",1,10,s_size1);
//			config1.extraParam(20, 2, 0.5, true, false, false);
//			qe.runExperiment(config1);
//			config1 = new Configuration("synt_db","unif",1,50,s_size1);
//			config1.extraParam(20, 2, 0.5, false, true, false);
//			qe.runExperiment(config1);
			
			//num of sources
//			int[] s_size1 = {20,30,40,50,60,70,80,90,100,110,120,130,140};
//			config1 = new Configuration("synt_db","unif",8,50,s_size1);
//			config1.extraParam(1,2,0.5,false,false,false);
//			qe.runExperiment(config1); //uniform data
//			config1.extraParam(10,2,0.5,false,false,false);
//			qe.runExperiment(config1); //uniform data
//			config1.extraParam(20,2,0.5,false,false,false);
//			qe.runExperiment(config1); //uniform data
//			config1.extraParam(30,2,0.5,false,false,false);
//			qe.runExperiment(config1); //uniform data
//			config1.extraParam(3,2,0.5,false,false,false);
//			qe.runExperiment(config1); //uniform data
			
			//varying degrees of data skew
//			int[] s_size1 = {20,80,140,200,260,320,380,440,500};
//			config1 = new Configuration("synt_db","unif",9,50,s_size1);
//			config1.extraParam(20,2,0,false,false,false); //1
//			qe.runExperiment(config1);
//			config1.extraParam(20,2,0.5,false,false,false); //0.6
//			qe.runExperiment(config1);
//			config1.extraParam(20,2,2,false,false,false); //0.13
//			qe.runExperiment(config1);
//			config1.extraParam(20,2,4,false,false,false); //0.018
//			qe.runExperiment(config1);
			
			//heatmap
			int[] s_size1 = incrementalSamples(200,1800,100);
			config1 = new Configuration("synt_db","unif",1,200,s_size1);
			config1.extraParam(20,2,0.7,false,false,true); //0.6 -> 0.3
			qe.runExperiment(config1);
			
			/** -------------- real-benchmark ------------ */
//			qe.runExperiment(config2); //synthetic gdp data
//			qe.runExperiment(config3); //real gdp data
//			qe.runExperiment(config4); //real employee data
//			qe.runExperiment(config5); //real EVM data (photon beam)
//			qe.runExperiment(config6); //real EVM data (Appendicitis)
//			qe.runExperiment(config7); //real SIGMOD/VLDB
			
			
		} 
		catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} 
	}
}
