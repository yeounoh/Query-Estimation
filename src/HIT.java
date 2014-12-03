
public class HIT {

	private String state;
	private int rank;
	private int gdp;
	private String assignID;
	private String workerID;
	private String HITId;
	private long acceptTime;
	
	public HIT(String[] ids, long time, String state, int gdp){
		this.state = state;
		this.gdp = gdp;
		this.assignID = ids[0];
		this.workerID = ids[1];
		this.HITId = ids[2];
		this.acceptTime = time;
	}
	
	public String getState(){
		return state;
	}
	
	public int getGDP(){
		return gdp;
	}
	
	public String getAssignID(){
		return assignID;
	}
	
	public String getWorkerID(){
		return workerID;
	}
	
	public String getHITId(){
		return HITId;
	}
	
	public long getAcceptTime(){
		return acceptTime;
	}
	
	public int getRank(){
		return rank;
	}
}
