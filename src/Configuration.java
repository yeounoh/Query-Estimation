
public class Configuration {
	String db_name;
	String tb_name;
	String[] tb_multi;
	int[] nb;
	int data_type;
	int n_rep;
	
	int base_est_type;
	
	//all data
	int[] s_size;
	
	//synthetic data experiment only
	int n_worker;
	int sampling_type;
	double lambda;
	double shape;
	boolean st_only = false;
	boolean st_inject = false;
	boolean heatmap = false;
	boolean bkt_exp = false;
	boolean n_src_exp = false;
	boolean normal_mean_exp = false;
	boolean pub_val_corr = true;
	
	public Configuration(String db_name, String tb_name, int data_type, int n_rep, int[] s_size){
		this.db_name = db_name;
		this.tb_name = tb_name;
		this.data_type = data_type;
		this.n_rep = n_rep;
		this.s_size = s_size; //total sample size
		this.shape = 1.0; // default, scaled exponential distribution.
	}
	
	public void extraParam(int n_worker, int sampling_type, double lambda, 
			boolean st_only, boolean st_inject, boolean heatmap){
		this.n_worker = n_worker; //(s_size/n_worker) samples per worker
		this.sampling_type = sampling_type; // 2-sampling without replacement
		this.lambda = lambda;
		this.st_only = st_only;
		this.st_inject = st_inject;
		this.heatmap = heatmap;
	}
	
	public void setShapeParam(double shape){
		this.shape = shape;
	}
	
	public void bucketExp(boolean bkt_exp){
		this.bkt_exp = bkt_exp;
	}
	
	public void normalMeanExp(boolean normal_mean_exp){
		this.normal_mean_exp = normal_mean_exp;
	}
	
	public void numSourceExp(boolean n_src_exp){
		this.n_src_exp = n_src_exp;
	}
	
	public void extraParam(String[] tb_multi){
		if(tb_multi.length != n_rep){
			System.err.print("(required) n_rep = tb_multi.length");
			System.exit(1);
		}
		this.tb_multi = tb_multi;
	}
	
	public void extraParam(int[] nb){
		this.nb = nb;
	}
	
	public void setPublicityValueCorr(boolean pb_corr){
		this.pub_val_corr = pb_corr;
	}
	
	public void setBaseEstimatorType(int type){
		this.base_est_type = type;
	}
}
