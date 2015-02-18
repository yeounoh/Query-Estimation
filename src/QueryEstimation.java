import java.awt.List;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;

// https://github.com/yeounoh/Query-Estimation.git
public class QueryEstimation {
	
	public static class Configuration {
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
	
	public static class Result {
		double[] cnt;
		double[] sum;
		double[] measure; 
		
		public Result(double[] cnt, double[] sum, double[] measure){
			this.cnt = cnt;
			this.sum = sum;
			this.measure = measure;
		}
		
		public double[] summary(){
			double[] summary = new double[cnt.length + sum.length + measure.length];
			int i= 0;
			for(double c : cnt)
				summary[i++] = c;
			for(double s : sum)
				summary[i++] = s;
			for(double m : measure)
				summary[i++] = m;
			return summary;
		}
		
		public String toString(){
			String str = "";
			for(double c : cnt)
				str += c + " ";
			for(double s : sum)
				str += s + " ";
			for(double m : measure)
				str += m + " ";
			return str;
		}
	}
	
	/**
	 * 
	 * @param sample
	 * @param n_bkt
	 * @param bkt_type: 1- ER fixed, 2- ER auto
	 * @param est_type: 0- chao92-based, 1- f1-based, 2- f12-based
	 * @return
	 */
	public Result bucketApproach(Object[] sample, int n_bkt, double th, int bkt_type, int est_type){
		Bucket[] buckets= null; //number of buckets may vary
		Estimator est = new Estimator(sample);
		
		if(bkt_type == 1){
			buckets = new Bucket[n_bkt];
			double width = (est.getMax() - est.getMin())/n_bkt; //System.out.println("width: "+width);
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
		
		double[] cnt = {cnt_t, uniq_t, chao_t, buckets.length};
		double[] sum = {sum_t};
		double[] measure = {avg_sc, avg_cv};
		
		return new Result(cnt, sum, measure);
	}
	
	public void runExperiment(Configuration conf) throws Exception{
		boolean do_gen = true;
		int n_class = 0; // ground truth for C (# of species), used for random sample generation
		String fname = "./result/";
		
		if(conf.data_type == 1 || conf.data_type == 2){
			n_class = conf.data_type == 1 ? 100 : 50;
			fname = conf.data_type == 1 ? fname + "uniform.txt" : fname + "syntGDP.txt";
		}
		else if(conf.data_type == 3 || conf.data_type == 4){
			fname = conf.data_type == 3 ? fname + "realGDP.txt" : fname + "solar.txt";
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
		
		for(int ri=0;ri<conf.n_rep;ri++){
			System.out.print("."); //progress meter
			for(int si=0;si<conf.s_size.length;si++){
				//data samples to run experiments
				Object[] sample = null;
				
				//synthetic data experiment
				if(conf.data_type == 1 || conf.data_type == 2){
					
					ArrayList<Object> samples = new ArrayList<Object>();
					for(int i=0;i<conf.n_worker;i++){
						Object[] s_worker = db.sampleByRandom((int) Math.ceil(conf.s_size[si]/conf.n_worker), 
								conf.tb_name, n_class, conf.sampling_type, conf.lambda); 
						for(Object s : s_worker){
							samples.add(s);
						}
					} 
					while(samples.size() > conf.s_size[si])
						samples.remove(samples.size()-1);
					sample = samples.toArray();
				}
				//real data experiment
				else if(conf.data_type == 3 || conf.data_type == 4){
					
					sample = db.sampleByTime(conf.s_size[si], conf.tb_name);
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
				
				//estimate population statistics from samples (method1: naive)
				naive_rep[si][ri] = naive.summary();
				naivef1_rep[si][ri] = naive_f1.summary();
				bkt_rep[si][ri] = bkt.summary();
				bktf1_rep[si][ri] = bktf1.summary();
				bkt_auto_rep[si][ri] = bkt_auto.summary();
				bktf1_auto_rep[si][ri] = bktf1_auto.summary();
			}
		}
	
		FileOutputStream fos= new FileOutputStream(fname);
		BufferedWriter bw= new BufferedWriter(new OutputStreamWriter(fos));
		bw.write("s_size | naive_avg | naive_std | naivef1 | naivef1_std | bkt_avg | bkt_std | bktf1_avg | bktf1_std | "
				+ "bktauto_avg | bktauto_std | bktf1auto_avg | bktf1auto_std | uniq_avg | uniq_std | nbktauto_avg | nbktf1auto_avg");
		bw.newLine();
		bw.flush();

		for(int si=0;si<conf.s_size.length;si++){
			double avg=0, avg_f1=0, avg_bkt=0, avg_bktf1=0, avg_bkt_auto=0, avg_bktf1_auto=0, avg_uniq=0;
			double std=0, std_f1=0, std_bkt=0, std_bktf1=0, std_bkt_auto=0, std_bktf1_auto=0, std_uniq=0, avg_nbktauto=0, avg_nbktf1auto=0;
			
			for(int ri=0;ri<conf.n_rep;ri++){
				avg += naive_rep[si][ri][4]/conf.n_rep;
				avg_f1 += naivef1_rep[si][ri][4]/conf.n_rep;
				avg_bkt += bkt_rep[si][ri][4]/conf.n_rep;
				avg_bktf1 += bktf1_rep[si][ri][4]/conf.n_rep;
				avg_bkt_auto += bkt_auto_rep[si][ri][4]/conf.n_rep;
				avg_bktf1_auto += bktf1_auto_rep[si][ri][4]/conf.n_rep;
				avg_uniq += naive_rep[si][ri][1]/conf.n_rep;
				avg_nbktauto += bkt_auto_rep[si][ri][3]/conf.n_rep;
				avg_nbktf1auto += bktf1_auto_rep[si][ri][3]/conf.n_rep;
			}
			
			for(int ri=0;ri<conf.n_rep;ri++){
				std += (naive_rep[si][ri][4]-avg)*(naive_rep[si][ri][4]-avg);
				std_f1 += (naivef1_rep[si][ri][4]-avg_f1)*(naivef1_rep[si][ri][4]-avg_f1);
				std_bkt += (bkt_rep[si][ri][4]-avg_bkt)*(bkt_rep[si][ri][4]-avg_bkt);
				std_bktf1 += (bktf1_rep[si][ri][4]-avg_bktf1)*(bktf1_rep[si][ri][4]-avg_bktf1);
				std_bkt_auto += (bkt_auto_rep[si][ri][4]-avg_bkt_auto)*(bkt_auto_rep[si][ri][4]-avg_bkt_auto);
				std_bktf1_auto += (bktf1_auto_rep[si][ri][4]-avg_bktf1_auto)*(bktf1_auto_rep[si][ri][4]-avg_bktf1_auto);
				std_uniq += (naive_rep[si][ri][1]-avg_uniq)*(naive_rep[si][ri][1]-avg_uniq);
			}
			std = Math.sqrt(std/conf.n_rep); std_f1 = Math.sqrt(std_f1/conf.n_rep); std_bkt = Math.sqrt(std_bkt/conf.n_rep);
			std_bktf1 = Math.sqrt(std_bktf1/conf.n_rep); std_bkt_auto = Math.sqrt(std_bkt_auto/conf.n_rep); 
			std_bktf1_auto = Math.sqrt(std_bktf1_auto/conf.n_rep); std_uniq = Math.sqrt(std_uniq/conf.n_rep);
			
			bw.write(conf.s_size[si] + " " + avg + " " + std + " " + avg_f1 + " " + std_f1
					+ " " + avg_bkt + " " + std_bkt + " " + avg_bktf1 + " " + std_bktf1 
					+ " " + avg_bkt_auto + " " + std_bkt_auto + " " + avg_bktf1_auto
					+ " " + std_bktf1_auto + " " + avg_uniq + " " + std_uniq + " " + avg_nbktauto + " " + avg_nbktf1auto);
			bw.newLine();
			bw.flush();
		}
	}
	
	public static void main(String[] args){
		//experimental setup
		int[] s_size1 = {20,80,140,200,260,320,380,440,500,560,620,680,740,800,860,920,980};
		int[] s_size2 = {20,40,60,80,100,120,140,160,180,200,220,240,260,280,300,320,340,360,380,400,420,440,460,480,500};
		Configuration config1 = new Configuration("synt_db","unif",1,200,s_size1);
		config1.extraParam(20, 1, 0);
		Configuration config2 = new Configuration("synt_db","gdp",2,200,s_size2);
		config2.extraParam(20, 2, 0.5);
		
		int[] s_size3 = new int[(499-20)/20];
		for(int i=0;i<s_size3.length-1;i++)
			s_size3[i] = 20 + i*20;
		s_size3[s_size3.length-1] = 499;
		int[] s_size4 = new int[(327-20)/20];
		for(int i=0;i<s_size4.length-1;i++)
			s_size4[i] = 20 + i*20;
		s_size4[s_size4.length-1] = 327;
		Configuration config3 = new Configuration("real_db","gdp",3,1,s_size3);
		Configuration config4 = new Configuration("real_db","solar",4,1,s_size4);
		
		//run experiments
		QueryEstimation qe = new QueryEstimation();
		try {
//			qe.runExperiment(config1); //uniform data
			qe.runExperiment(config2); //synthetic gdp data
//			qe.runExperiment(config3); //real gdp data
//			qe.runExperiment(config4); //real solar data
		} 
		catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} 
	}
}
