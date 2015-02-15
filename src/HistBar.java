class HistBar {
	private double lower_b;
	private double upper_b;
	private int count;
	
	HistBar(double lb, double ub, int count){
		this.lower_b = lb;
		this.upper_b = ub;
		this.count = count;
	}
	
	int getCount(){
		return count;
	}
	
	void setCount(int c){
		count = c;
	}
	
	double getLowerB(){
		return lower_b;
	}
	
	double getUpperB(){
		return upper_b;
	}
	
	void setLowerB(double lb){
		if(lower_b > lb)
			lower_b = lb;
	}
	
	void setUpperB(double ub){
		if(upper_b < ub)
			upper_b = ub;
	}
}
