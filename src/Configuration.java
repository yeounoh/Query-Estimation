
public class Configuration {
	String db_name;
	String tb_name;
	String[] tb_multi;
	int data_type;
	int n_rep;
	
	//all data
	int[] s_size;
	
	//synthetic data experiment only
	int n_worker;
	int sampling_type;
	double lambda;
	boolean st_only = false;
	boolean st_inject = false;
	boolean heatmap = false;
	//boolean mc_index = false;
	boolean pub_val_corr = true;
	
	public Configuration(String db_name, String tb_name, int data_type, int n_rep, int[] s_size){
		this.db_name = db_name;
		this.tb_name = tb_name;
		this.data_type = data_type;
		this.n_rep = n_rep;
		this.s_size = s_size; //total sample size
	}
	
	public void extraParam(int n_worker, int sampling_type, double lambda, boolean st_only, boolean st_inject, boolean heatmap){
		this.n_worker = n_worker; //(s_size/n_worker) samples per worker
		this.sampling_type = sampling_type;
		this.lambda = lambda;
		this.st_only = st_only;
		this.st_inject = st_inject;
		this.heatmap = heatmap;
		//this.mc_index = mc_index;
	}
	
	public void extraParam(String[] tb_multi){
		if(tb_multi.length != n_rep){
			System.err.print("(required) n_rep = tb_multi.length");
			System.exit(1);
		}
		this.tb_multi = tb_multi;
	}
	
	public void setPublicityValueCorr(boolean pb_corr){
		this.pub_val_corr = pb_corr;
	}
}
