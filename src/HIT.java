
public class HIT {

	private String name;
	private double val;
	private String assignID;
	private String workerID;
	private String HITId;
	private long acceptTime;
	
	public HIT(String[] ids, long time, String name, double val){
		this.name = name;
		this.val = val;
		this.assignID = ids[0];
		this.workerID = ids[1];
		this.HITId = ids[2];
		this.acceptTime = time;
	}
	
	public String getName(){
		return name;
	}
	
	public double getValue(){
		return val;
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
}
