
public class HIT {

	private String key;
	private String val;
	private String assignID;
	private String workerID;
	private String HITId;
	private long acceptTime;
	private long submitTime;
	private long timeToComplete;
	
	public HIT(String[] ids, long[] times, String key, String val){
		this.key = key;
		this.val = val;
		this.assignID = ids[0];
		this.workerID = ids[1];
		this.HITId = ids[2];
		this.acceptTime = times[0];
		this.submitTime = times[1];
		this.timeToComplete = times[2];
	}
	
	public String getKey(){
		return key;
	}
	
	public String getVal(){
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
	
	public long getSubmitTIme(){
		return submitTime;
	}
	
	public long getTimeToComplete(){
		return timeToComplete;
	}
}
