import java.awt.List;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

// https://github.com/yeounoh/Query-Estimation.git
public class QueryEstimation {
	
	public static int[] incrementalSamples(int t, int inc){
		int[] s_size = new int[(t-inc)/inc];
		for(int i=0;i<s_size.length-1;i++)
			s_size[i] = inc + i*inc;
		s_size[s_size.length-1] = t;
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
		double avg_t = 0;
		for(int bi=0;bi<buckets.length;bi++){
			Object[] samples_b = buckets[bi].getSamples().toArray();
			est = new Estimator(samples_b);
			
			cnt_by_bucket[bi] = samples_b.length; 
			sum_by_bucket[bi] = est_type == 1 ? est.sumEst() : est.sumf1(); 
			sc_by_bucket[bi] = buckets[bi].getSampleCov();
			cv_by_bucket[bi] = buckets[bi].getCoeffVar();
			chao_by_bucket[bi] = est.chao92();
			csum_by_bucket[bi] = est.chao84();//est.csum();
			
			cnt_t += cnt_by_bucket[bi];
			uniq_t += est.getUniqueCount();
			chao_t += chao_by_bucket[bi];
			sum_t += sum_by_bucket[bi];
			csum_t += csum_by_bucket[bi];
			avg_sc += sc_by_bucket[bi] * (double) samples_b.length/sample.length; 
			avg_cv += cv_by_bucket[bi] * (double) samples_b.length/sample.length;
		}
		avg_t = sum_t/chao_t;
		double max_orig = est.getMax();
		double max_est = buckets[buckets.length-1].maxEst();
		double[] cnt = {cnt_t, uniq_t, chao_t, buckets.length};
		double[] sum = {sum_t, csum_t};
		double[] measure = {avg_sc, avg_cv};
		double[] other = {avg_t, max_orig, max_est};
		
		return new Result(cnt, sum, measure, other);
	}
	
	public Result MonteCarloApproach(Object[] sample, int[] n_w, int sweep){
		Estimator est = new Estimator(sample);
		int c_hat = 100;//(int) Math.ceil(est.chao92());
		double[] error = new double[sweep/2*2];
		double[] sum_est = new double[sweep/2*2];
		double[] cnts = new double[sweep/2*2];
		for(int i=c_hat-sweep/2;i<c_hat+sweep/2;i++){
			cnts[i-(c_hat-sweep/2)] = i;
			error[i-(c_hat-sweep/2)] = est.MonteCarlo(i, n_w, 2);
			sum_est[i-(c_hat-sweep/2)] = est.sumEst(i);
		}
		double min_err = Double.MAX_VALUE, best_est = 0, best_cnt= 0;
		for(int i=0;i<error.length;i++){
			if(min_err > error[i]){
				best_cnt = cnts[i];
				min_err = error[i];
				best_est = sum_est[i];
			}
		}
		
		double[] cnt = {best_cnt};
		double[] sum = {best_est};
		double[] measure = {};
		double[] other = {best_est/best_cnt};
		
		return new Result(cnt, sum, measure, other);
	}
	
	public Result MonteCarloSimulation(Object[] sample, int[] n_w, int sweep){
		Estimator est = new Estimator(sample);
		int c_hat = (int) Math.ceil(est.chao92());
		double[] cnts = new double[(sweep/2*2)];
		double[] error = new double[(sweep/2*2)];
		double[] sum_est = new double[(sweep/2*2)];
		for(int i=c_hat-sweep/2;i<c_hat+sweep/2;i++){
			cnts[i-(c_hat-sweep/2)] = i;
			error[i-(c_hat-sweep/2)] = est.MonteCarlo(i, n_w, 2);
			sum_est[i-(c_hat-sweep/2)] = est.sumEst(i);
		}
		
		double[] cnt = cnts;
		double[] sum = sum_est;
		double[] measure = error;
		double[] other = {};
		
		return new Result(cnt, sum, measure, other);
	}
	
	public void runExperiment(Configuration conf, int mc_simul) throws Exception{
		boolean do_gen = true;
		int n_class = 0; // ground truth for C (# of species), used for random sample generation
		String fname = "./result/";
		switch (conf.data_type) {
			case 1: fname += "uniform.txt"; break;
			case 2: fname += "syntGDP.txt"; break;
			case 3: fname += "realGDP.txt"; break;
			case 4: fname += "solar.txt"; break;
			case 5: fname += "evm.txt"; break;
			case 6: fname += "evm_app.txt"; break;
			default: fname += "N/S.txt"; break;
		}
		
		if(conf.data_type == 1 || conf.data_type == 2){
			n_class = conf.data_type == 1 ? 100 : 50;
		}
		
		Database db = new DataGenerator().generateDataset(conf.db_name, conf.tb_name, do_gen, conf.data_type);
		
		//method1: naive
		double[][][] naive_rep = new double[conf.s_size.length][conf.n_rep][]; 
		double[][][] naivef1_rep = new double[conf.s_size.length][conf.n_rep][];
		
		//method2: bucket
		double[][][] bkt_rep = new double[conf.s_size.length][conf.n_rep][]; 
		double[][][] bktf1_rep = new double[conf.s_size.length][conf.n_rep][];
		double[][][] bkt_auto_rep = new double[conf.s_size.length][conf.n_rep][]; 
		double[][][] bktf1_auto_rep = new double[conf.s_size.length][conf.n_rep][];
		
		//method3: Monte-Carlo Simulation
		double[][][] mc_app_rep = new double[conf.s_size.length][conf.n_rep][];
		double[][][] mc_rep = mc_simul == 1? new double[conf.s_size.length][conf.n_rep][] : null;
		int sweep = 20;
		
		for(int ri=0;ri<conf.n_rep;ri++){
			System.out.print("."); //progress meter
			for(int si=0;si<conf.s_size.length;si++){
				//data samples to run experiments
				Object[] sample = null;
				int[] n_w = null;
				int assigned_sample = 0;
				
				//synthetic data experiment
				if(conf.data_type == 1 || conf.data_type == 2){
					n_w = new int[conf.n_worker];
					ArrayList<Object> samples = new ArrayList<Object>();
					
					boolean streaker_only = false;
					boolean inject_streaker = false;
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
				Result naive = bucketApproach(sample, 1, 0.0, 1, 0);
				Result naive_f1 = bucketApproach(sample, 1, 0.0, 1, 1);
				
				//ER fixed approach
				Result bkt = bucketApproach(sample, 5, 0.0, 1, 0);
				Result bktf1 = bucketApproach(sample, 5, 0.0, 1, 1);
				
				//ER auto approach
				Result bkt_auto = bucketApproach(sample, 0, 0.05, 2, 0);
				Result bktf1_auto = bucketApproach(sample, 0, 0.05, 2, 1);
				
				//Monte-Carlo Simulation
				Result mc = mc_simul == 1? MonteCarloSimulation(sample, n_w, sweep) : null;
				Result mc_app = MonteCarloApproach(sample, n_w, sweep);
				
				//estimate population statistics
				naive_rep[si][ri] = naive.summary();
				naivef1_rep[si][ri] = naive_f1.summary();
				bkt_rep[si][ri] = bkt.summary();
				bktf1_rep[si][ri] = bktf1.summary();
				bkt_auto_rep[si][ri] = bkt_auto.summary();
				bktf1_auto_rep[si][ri] = bktf1_auto.summary();
				if(mc_simul == 1){
					mc_rep[si][ri] = mc.summary();
				}
				mc_app_rep[si][ri] = mc_app.summary();
			}
		}
	
		FileOutputStream fos= new FileOutputStream(fname);
		BufferedWriter bw= new BufferedWriter(new OutputStreamWriter(fos));
		bw.write("0:s_size | 1:naive_avg | 2:naive_std | 3:naivef1 | 4:naivef1_std | 5:bkt_avg | 6:bkt_std | 7:bktf1_avg | 8:bktf1_std | "
				+ "9:bktauto_avg | 10:bktauto_std | 11:bktf1auto_avg | 12:bktf1auto_std | 13:uniq_avg | 14:uniq_std | 15:nbktauto_avg | 16:nbktf1auto_avg |"
				+ "17:mc_avg | 18:mc_std | 19:chao_avg | 20:chao_std | 21:csum_avg | 22:csum_std | 23:naive_avg_avg | 24:naivef1_avg_avg |"
				+ "25:bkt_avg_avg | 26:bktf1_avg_avg | 27:bktauto_avg_avg | 28:bktf1auto_avg_avg | 29:mc_avg_avg | 30:max_orig_avg | 31:max_est_avg |");
		bw.newLine();
		bw.flush();
		
		//Monte-Carlo simulation
		FileOutputStream fos_mc= mc_simul == 1? new FileOutputStream(fname+"_mc") : null;
		BufferedWriter bw_mc= mc_simul == 1? new BufferedWriter(new OutputStreamWriter(fos_mc)) : null;
		if(mc_simul == 1){
			bw_mc.write("s_size | cnts ... | sums ... | errs ... |");
			bw_mc.newLine();
			bw_mc.flush();
		}
		for(int si=0;si<conf.s_size.length;si++){
			double avg_csum=0, std_csum=0;
			double avg=0, avg_f1=0, avg_bkt=0, avg_bktf1=0, avg_bkt_auto=0, avg_bktf1_auto=0, avg_uniq=0, avg_chao=0, avg_mc_app=0;
			double std=0, std_f1=0, std_bkt=0, std_bktf1=0, std_bkt_auto=0, std_bktf1_auto=0, std_uniq=0, std_chao=0, std_mc_app=0;
			double avg_nbktauto=0, avg_nbktf1auto=0, avg_max_orig=0, avg_max_est=0;
			
			double[] avg_mc_cnt = mc_simul == 1 ? new double[sweep] : null;
			double[] avg_mc_sum = mc_simul == 1 ? new double[sweep] : null;
			double[] avg_mc_err = mc_simul == 1 ? new double[sweep] : null;
			
			double avg_avg=0, avg_avg_f1=0, avg_avg_bkt=0, avg_avg_bktf1=0, avg_avg_bkt_auto=0,avg_avg_bktf1_auto=0,avg_avg_mc_app=0;
			for(int ri=0;ri<conf.n_rep;ri++){
				avg_csum += naive_rep[si][ri][5]/conf.n_rep;
				avg += naive_rep[si][ri][4]/conf.n_rep;
				avg_avg += naive_rep[si][ri][8]/conf.n_rep;
				avg_f1 += naivef1_rep[si][ri][4]/conf.n_rep;
				avg_avg_f1 += naivef1_rep[si][ri][8]/conf.n_rep;
				avg_bkt += bkt_rep[si][ri][4]/conf.n_rep;
				avg_avg_bkt += bkt_rep[si][ri][8]/conf.n_rep;
				avg_bktf1 += bktf1_rep[si][ri][4]/conf.n_rep;
				avg_avg_bktf1 += bktf1_rep[si][ri][8]/conf.n_rep;
				avg_bkt_auto += bkt_auto_rep[si][ri][4]/conf.n_rep;
				avg_avg_bkt_auto += bkt_auto_rep[si][ri][8]/conf.n_rep;
				avg_bktf1_auto += bktf1_auto_rep[si][ri][4]/conf.n_rep;
				avg_avg_bktf1_auto += bktf1_auto_rep[si][ri][8]/conf.n_rep;
				avg_uniq += naive_rep[si][ri][1]/conf.n_rep;
				avg_nbktauto += bkt_auto_rep[si][ri][3]/conf.n_rep;
				avg_nbktf1auto += bktf1_auto_rep[si][ri][3]/conf.n_rep;
				avg_chao += naive_rep[si][ri][2]/conf.n_rep;
				avg_mc_app += mc_app_rep[si][ri][1]/conf.n_rep;
				avg_avg_mc_app += mc_app_rep[si][ri][2]/conf.n_rep;
				avg_max_orig += bkt_auto_rep[si][ri][9]/conf.n_rep;
				avg_max_est += bkt_auto_rep[si][ri][10]/conf.n_rep;
				
				if(mc_simul == 1){
					for(int i=0;i<sweep;i++){
						avg_mc_cnt[i] += mc_rep[si][ri][i]/conf.n_rep;
						avg_mc_sum[i] += mc_rep[si][ri][i+sweep]/conf.n_rep;
						avg_mc_err[i] += mc_rep[si][ri][i+(2*sweep)]/conf.n_rep; 
					}
				}
			}
			
			for(int ri=0;ri<conf.n_rep;ri++){
				std_csum += (naive_rep[si][ri][5]-avg_csum)*(naive_rep[si][ri][5]-avg_csum);
				std += (naive_rep[si][ri][4]-avg)*(naive_rep[si][ri][4]-avg);
				std_f1 += (naivef1_rep[si][ri][4]-avg_f1)*(naivef1_rep[si][ri][4]-avg_f1);
				std_bkt += (bkt_rep[si][ri][4]-avg_bkt)*(bkt_rep[si][ri][4]-avg_bkt);
				std_bktf1 += (bktf1_rep[si][ri][4]-avg_bktf1)*(bktf1_rep[si][ri][4]-avg_bktf1);
				std_bkt_auto += (bkt_auto_rep[si][ri][4]-avg_bkt_auto)*(bkt_auto_rep[si][ri][4]-avg_bkt_auto);
				std_bktf1_auto += (bktf1_auto_rep[si][ri][4]-avg_bktf1_auto)*(bktf1_auto_rep[si][ri][4]-avg_bktf1_auto);
				std_uniq += (naive_rep[si][ri][1]-avg_uniq)*(naive_rep[si][ri][1]-avg_uniq);
				std_chao += (naive_rep[si][ri][2]-avg_chao)*(naive_rep[si][ri][2]-avg_chao);
				std_mc_app += (mc_app_rep[si][ri][0]-avg_mc_app)*(mc_app_rep[si][ri][0]-avg_mc_app);
			}
			std_csum = Math.sqrt(std_csum/conf.n_rep);
			std = Math.sqrt(std/conf.n_rep); std_f1 = Math.sqrt(std_f1/conf.n_rep); std_bkt = Math.sqrt(std_bkt/conf.n_rep);
			std_bktf1 = Math.sqrt(std_bktf1/conf.n_rep); std_bkt_auto = Math.sqrt(std_bkt_auto/conf.n_rep); 
			std_bktf1_auto = Math.sqrt(std_bktf1_auto/conf.n_rep); std_uniq = Math.sqrt(std_uniq/conf.n_rep);
			std_chao = Math.sqrt(std_chao/conf.n_rep); std_mc_app = Math.sqrt(std_mc_app/conf.n_rep);
			
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
					+ " " + df.format(avg_avg) + " " + df.format(avg_avg_f1) + df.format(avg_avg_bkt) + df.format(avg_avg_bktf1)
					+ " " + df.format(avg_avg_bkt_auto) + df.format(avg_avg_bktf1_auto) + df.format(avg_avg_mc_app)
					+ " " + df.format(avg_max_orig) + " " + df.format(avg_max_est));
			bw.newLine();
			bw.flush();
			
			if(mc_simul == 1){
				String mc_cnt=""; String mc_sum =""; String mc_err = ""; 
				for(int i=0;i<sweep;i++){
					mc_cnt += df.format(avg_mc_cnt[i]) + " ";
					mc_sum += df.format(avg_mc_sum[i]) + " ";
					mc_err += df.format(avg_mc_err[i]) + " ";
				}
				bw_mc.write(conf.s_size[si] + " " + mc_cnt + " " + mc_sum + " " + mc_err);
				bw_mc.newLine();
				bw_mc.flush();
			}
		}
		bw.close();
		if(mc_simul == 1)
			bw_mc.close();
	}
	
	public static void main(String[] args){
		//experimental setup: Configuration(String db_name, String tb_name, int data_type, int n_rep, int[] s_size)
		int[] s_size1 = {20,80,140,200,260,320,380,440,500,560,620,1000};//{10,20,30,40,50,60,70,80,90,100};//{20,40,60,80,100,120,140,160,180,200,220,240,260,280,300,320,340,360,380,400,420,440,460,480};//
		int[] s_size2 = {20,40,60,80,100,120,140,160,180,200,220,240,260,280,300,320,340,360,380,400,420,440,460,480,500};
		Configuration config1 = new Configuration("synt_db","unif",1,100,s_size1);
		config1.extraParam(20, 2, 0.5); //change the last parameter to adjust the correlation level
		Configuration config2 = new Configuration("synt_db","gdp",2,100,s_size2);
		config2.extraParam(20, 2, 0.5);
		
		int[] s_size3 = incrementalSamples(496,20);
		int[] s_size4 = incrementalSamples(327,20);
		int[] s_size5 = incrementalSamples(812,20);
		int[] s_size6 = incrementalSamples(6160,40);
		
		Configuration config3 = new Configuration("real_db","gdp",3,1,s_size3);
		Configuration config4 = new Configuration("real_db","solar",4,1,s_size4);
		Configuration config5 = new Configuration("real_db","evm",5,1,s_size5);
		Configuration config6 = new Configuration("real_db","evm",6,1,s_size6);
		
		//run experiments
		QueryEstimation qe = new QueryEstimation();
		try {
			qe.runExperiment(config1,0); //uniform data
//			qe.runExperiment(config2,0); //synthetic gdp data
//			qe.runExperiment(config3,0); //real gdp data
//			qe.runExperiment(config4,0); //real solar data
//			qe.runExperiment(config5,0); //real EVM data (photon beam)
//			qe.runExperiment(config6,0); //real EVM data (Appendicitis)
		} 
		catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} 
	}
}
