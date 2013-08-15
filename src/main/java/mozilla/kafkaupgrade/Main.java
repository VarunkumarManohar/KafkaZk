package mozilla.kafkaupgrade;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class Main {

	public static void main(String[] args) throws NumberFormatException, IOException, InterruptedException, ExecutionException {
		
		ZookeeperQuery qObj = new ZookeeperQuery();
		List<String> topics	=qObj.queryTopics();
		
		
		System.out.println("These are the following available topics");
		int index =1;
		for(String s :topics)
		{
			System.out.println(index+")"+s);
			index++;
					
		}
		index=1;
		
		System.out.println("Select a topic");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		Integer topicIndex = Integer.parseInt(br.readLine())-1;
		
		List<String>brokerNodes	=qObj.topicBrokers(topics.get(topicIndex));
		
		System.out.println(topics.get(topicIndex)+" is avaiable  on the following brokers"  );
		for(String s :brokerNodes)
		{
			System.out.println( index+")" +"Broker"+s);
			index++;
					
		}
		System.out.println("Select a broker");
		
		br = new BufferedReader(new InputStreamReader(System.in));
		Integer brokerIndex = Integer.parseInt(br.readLine())-1;
		
		
		Integer totalPartitoins =qObj.brokerPartitions(topics.get(topicIndex), brokerNodes.get(brokerIndex));
		System.out.println("there are " +totalPartitoins +"  partitions for the topic  "+ topics.get(topicIndex));
		

		System.out.println("How many days do you want to go back in time ? !!!");
		InputStreamReader ir = new InputStreamReader(System.in);

		br = new BufferedReader(ir);
		int noDays = Integer.parseInt(br.readLine());
		
		List<ListenableFuture<Long>> computationResults = new ArrayList<ListenableFuture<Long>>();
		//ExecutorService executor = Executors.newFixedThreadPool(5);
		ListeningExecutorService pool= MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));
		
		
		for(int i =0;i<totalPartitoins;i++)
		{
			/*create a callable object and submit to the exectuor to trigger of a execution */
			
			   Callable<Long>callable = new KafkaQueueOps(brokerNodes.get(brokerIndex), topics.get(topicIndex), i, noDays);
			   final ListenableFuture<Long> future=pool.submit(callable);
			   computationResults.add(future);
				
			   
			
		}
		
		ListenableFuture<List<Long>> successfulResults= Futures.successfulAsList(computationResults);	
		
		
		
		/*Futures.addCallback(successfulResults, new FutureCallback<List<Long>>() {

			public void onFailure(Throwable arg0) {
				// TODO Auto-generated method stub
				
			}

		

			public void onSuccess(List<Long> arg0) {
				// TODO Auto-generated method stub
				
				
				System.out.println("here");
				Long totalCount=0L;
				for(Long i : arg0)
					totalCount+=i;
				
				System.out.println(totalCount);
				
			}
		});
		
		*/
		
		
		List<Long> results =successfulResults.get();
		
		Long totalCount=0L;
		for(Long i : results)
			totalCount+=i;
		
		System.out.println(totalCount);
		
		//pseudocode
		/*for each partition of topic 
		 * {
		 * 		executor.submit (callable ---> that uses kafka api getoffsetsbefore and other api
		 * }
		 */
		
		
	}
	
}
