package DATA.DATA;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import twitter4j.*;
import twitter4j.conf.*;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import kafka.producer.KeyedMessage;



public class KafkaTwitterProducer {
	public static void main(String[] args) throws Exception {
		final LinkedBlockingQueue<Status> q = new LinkedBlockingQueue<Status>(1000);

		if (args.length < 4) {
			System.out.println(
					"Veuillez entrer les arguments suivants: twitter-consumer-secret twitter-access-token  topic-name twitter-search-keywords");
			return;
		}

		String consumerKey = args[0].toString();
		String consumerSecret = args[1].toString();
		String accessToken = args[2].toString();
		String accessTokenSecret = args[3].toString();
		String topicName = args[4].toString();
		String[] arguments = args.clone();
		String[] keyWords = Arrays.copyOfRange(arguments, 5, arguments.length);
		String[]Location= {"Paris", "New York", "Marrakech","Casablanca","London"};

		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret)
				.setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret);

		// Creation de twitterstream 
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		StatusListener listener = new StatusListener() {

			public void onStatus(Status status) {
				q.offer(status);
			}

			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("limitated Statuses:" + numberOfLimitedStatuses);
			}

			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("userId:" + userId + "upToStatusId:" + upToStatusId);
			}

			public void onStallWarning(StallWarning warning) {
				System.out.println("Warning:" + warning);
			}

			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};
		twitterStream.addListener(listener);

		// Filtrer les keywords
		FilterQuery query = new FilterQuery().track(keyWords);
		twitterStream.filter(query);
		
		// Filtrer par location
		FilterQuery query1 = new FilterQuery().track(Location);
		twitterStream.filter(query1);

		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		int i = 0;
		int j = 0,k=0;

		
		//envoyer les tweets. si les nvx tweets arrivent on les envoie au topic
		while (true) {
			Status sts = q.poll();
			
			if (sts == null) {
				Thread.sleep(100);
				// i++;
			} else {
				for (HashtagEntity hashtage : sts.getHashtagEntities()) {
					System.out.println("Producer" +k+" :");
					k++;

					System.out.println("Tweet:" +sts);
					System.out.println("Hashtag: " + hashtage.getText());
					producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(j++), sts.getText()));
				}
			}			
		}
		
		
	}

}