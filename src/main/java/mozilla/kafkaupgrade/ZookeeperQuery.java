package mozilla.kafkaupgrade;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;



import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class ZookeeperQuery {

	private static ZkClient client;
	String CONSUMERS_PATH = "/consumers";
	String BROKER_IDS_PATH = "/brokers/ids";
	static String BROKER_TOPICS_PATH = "/brokers/topics";
	
	String topicSelctd="";
	String brkrSelctd="";
	Integer prtnSlctd;	    
	Map<String, String> brokers ;
	
	
	static class StringSerializer implements ZkSerializer 
	{

        public StringSerializer() {}
        public Object deserialize(byte[] data) throws ZkMarshallingError {
            if (data == null) return null;
            return new String(data);
        }

        public byte[] serialize(Object data) throws ZkMarshallingError {
            return data.toString().getBytes();
        }
    }

	  private String getOffsetsPath(String group) {
	      String brokerId= "0";  
		  return CONSUMERS_PATH + "/" + group + "/offsets/" + topicSelctd +"/"+this.brkrSelctd+"-"+prtnSlctd;
	  }
	
    private List<String> getChildrenParentMayNotExist(String path) {
        try {
            List<String> children = client.getChildren(path);
            return children;
        } catch (ZkNoNodeException e) {
            return new ArrayList<String>();
        }
    }
    
  /*/brokers/topics/topic1/lst_of_brokers for the topic */
    
    public List<String> getTopics(String path)
    {
    	   List<String> topics = getChildrenParentMayNotExist(path);
    	   return topics;
    		
    }
    
    
    public List<String> getBrokers(String path) {
        
        List<String> brokers = getChildrenParentMayNotExist( path);
        return brokers;
    }
    
    
    public Integer getBrokerData(String path)
    {
    	Integer	parts = Integer.parseInt((String)client.readData(path));

      
    	return parts;
    }
	
    public List<String> queryTopics()
    {
    	
    	try
    	{
    		client = new ZkClient("node8.testing.stage.metrics.scl3.mozilla.com:2181", 6000,1000000 , new StringSerializer() );
    		System.out.println("Connected to zookeeper");
    	
    	}
    	catch(Exception e)
    	{
    		System.out.println("Unable to connect to the zookeeper");
    		
    		e.printStackTrace();
    	
    		System.exit(0);
    	}
    	List<String> topics=getTopics(BROKER_TOPICS_PATH);
		return topics;
    	
    }
	public List<String> topicBrokers(String topic)
	{
		List<String> brokers=getBrokers(BROKER_TOPICS_PATH+"/"+topic);
		
		return brokers;
	}
	
	public Integer brokerPartitions(String topic, String broker)
	{
		
		  Integer partitions=  getBrokerData(BROKER_TOPICS_PATH+"/"+topic+"/"+broker);
	
		  return partitions;
	
	}
	
	
   
    
}
