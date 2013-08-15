package mozilla.kafkaupgrade;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import com.google.protobuf.ByteString;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;

import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

public class KafkaQueueOps implements Callable {
	
		String topicName;
		String brokerNode;
		Integer partitionNumber;
		Integer noDays;
		
		public KafkaQueueOps(String brokerNode,String topicName,Integer partitionNumber, Integer noDays) {
			// TODO Auto-generated constructor stub
			
			this.topicName=topicName;
			this.brokerNode=brokerNode;
			this.partitionNumber=partitionNumber;
			this.noDays=noDays;
		}
		
		public Long call() throws Exception {
			// TODO Auto-generated method stub
			
			String brokerName= "node"+brokerNode+".testing.stage.metrics.scl3.mozilla.com";
			SimpleConsumer consumer = new SimpleConsumer(brokerName, 9092, 10000,
					Integer.MAX_VALUE);
			long[] offLst1 = consumer.getOffsetsBefore(topicName, partitionNumber, -1,
					Integer.MAX_VALUE);
			
			
			
			List<Long> offLst = new ArrayList<Long>();
			for (long off : offLst1)
				offLst.add(off);
			
			long offset = offLst.get(1);
			long markedOffst = 0;
			long currentOffst = 0;

			long offstIndex = 0;

			/* checking the sparse list of offsets for fixing the go-back point */
			for (int i = 1; i < offLst.size(); i++) {
				FetchRequest fr = new FetchRequest(topicName, partitionNumber, offLst.get(i),
						Integer.MAX_VALUE);
				ByteBufferMessageSet message = consumer.fetch(fr);
				Date d1 = new Date();

				for (MessageAndOffset msg : message) {
					
					BagheeraMessage bmsg=BagheeraMessage.parseFrom(ByteString.copyFrom(msg.message().payload()));

					
					String dateTemp = new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new java.util.Date (bmsg.getTimestamp()));
					Date d2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(dateTemp);
					
					long diffVal = d1.getTime() - d2.getTime();
					
					long MILLIS_PER_DAY = 24 * 3600 * 1000;
					long dayDiff = Math.round(diffVal / ((double) MILLIS_PER_DAY));

					if (!(dayDiff <= noDays)) {
						/* I want to go back like three days but currently i am two days back - so consider the 2nd back day
						 * if not the number of days are greater than the time you
						 * want to go back
						 */
						/* =>stop */
						markedOffst = offLst.get(i);
						currentOffst = offLst.get(i);
						offstIndex = i;

						break;
					} else {
						
						//I want to go back like three days but currently i am two days back - so consider the 2nd back day
						// offset=msg.offset();
						currentOffst = offLst.get(i);
						offstIndex = i;
					}

				}

				if (markedOffst > 0)
					break;

			}
			long checkpoint = offstIndex;
			long offsetFinal = offLst.get((int) (checkpoint));
			long endPoint = offLst.get(1);
			/* offLst.get(1) will have the highest value of the offset */
			
			long totalCount=0;
			while (offsetFinal <= offLst.get(1)) {
				// fetch the messages starting from offsetFinal
				FetchRequest fetchRequest = new FetchRequest(topicName, partitionNumber,
						offsetFinal, Integer.MAX_VALUE);
				ByteBufferMessageSet messages = consumer.fetch(fetchRequest);

				for (MessageAndOffset msg : messages) {
				
					BagheeraMessage bmsg=BagheeraMessage.parseFrom(ByteString.copyFrom(msg.message().payload()));

					
					String dateTemp = new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new java.util.Date (bmsg.getTimestamp()));
					Date d2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(dateTemp);
					Date d1 = new Date();
					long diffVal = d1.getTime() - d2.getTime();
				
					long MILLIS_PER_DAY = 24 * 3600 * 1000;
					long dayDiff = Math.round(diffVal / ((double) MILLIS_PER_DAY));

					
					if(dayDiff<=noDays)
						totalCount++;
					
						
					offsetFinal = msg.offset();
					//bw.write(Utils.toString(msg.message().payload(), "UTF-8"));
					//bw.write("\n");
				}
			}
			
			
		
		
			return totalCount;
		
		
		}
	
		
}
