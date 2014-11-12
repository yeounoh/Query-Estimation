
public class State {
	private String name;
	private int rank; //unique id
	private int gdp;
	
	public State(String name, int rank, int gdp){
		this.name = name;
		this.rank = rank;
		this.gdp = gdp;
	}
	
	public String getName(){
		return name;
	}
	
	public int getRank(){
		return rank;
	}
	
	public int getGDP(){
		return gdp;
	}
}