package cl.citiaps.twitter.streaming;

import java.io.IOException;
import java.util.*;

import com.mongodb.client.MongoCollection;
import org.apache.commons.io.IOUtils;

import org.bson.Document;
import twitter4j.*;

public class TwitterStreaming {

	private final TwitterStream twitterStream;
	private MongoConnection conn;
	private MongoCollection<Document> collection;
	private Set<String> keywords;

	private TwitterStreaming() {
		this.twitterStream = new TwitterStreamFactory().getInstance();
		this.keywords = new HashSet<>();
		loadKeywords();

		/* Se crea la conexión con MongoDB y se almacena la colección de Tweets */
		this.conn = new MongoConnection("127.0.0.1", 27017, "TBD", "tweets");
		this.collection = this.conn.getCollection();
	}

	private void loadKeywords() {
		try {
			ClassLoader classLoader = getClass().getClassLoader();
			keywords.addAll(IOUtils.readLines(classLoader.getResourceAsStream("words.dat"), "UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void init() {

		StatusListener listener = new StatusListener() {

			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
			}

			public void onException(Exception ex) {
				ex.printStackTrace();
			}

			@Override
			public void onStallWarning(StallWarning arg0) {

			}

			@Override
			public void onStatus(Status status) {

				// Se almacena el país desde donde se realizó el tweet
				String country;
				try {
					country = status.getUser().getLocation();
				} catch (NullPointerException e){
					country = "none";
				}

				//  Se obtiene la fecha en que se recuperan el o los tweets.
				Calendar cal = Calendar.getInstance();
				int year = cal.get(Calendar.YEAR);
				int month = cal.get(Calendar.MONTH) + 1;
				int day = cal.get(Calendar.DAY_OF_MONTH);
				int hour = cal.get(Calendar.HOUR_OF_DAY);
				int min = cal.get(Calendar.MINUTE);

				//Geolocalización del tweet
				String latitude;
				String longitude;
				try {
					latitude = Double.toString(status.getGeoLocation().getLatitude());
					longitude = Double.toString(status.getGeoLocation().getLongitude());
				} catch (NullPointerException e){
					latitude = "none";
					longitude = "none";
				}


				// Se obtienen la cantidad de hashtags
				int hCount = status.getHashtagEntities().length;
				List<String> hashtags = new ArrayList<>();
				if(hCount > 0){
					// Se agregan todos los hashtags a un arreglo
					for(TweetEntity hashtag : status.getHashtagEntities()){
						hashtags.add(hashtag.getText());
					}
				}

				// Se crea un documento que representa el JSON que se almacenará en MongoDB
				Document tweet = new Document("id", status.getId())
									  .append("user", status.getUser().getScreenName())
									  .append("name", status.getUser().getName())
									  .append("tweetText", status.getText())
									  .append("hashtags", hashtags)
									  .append("day", day)
									  .append("month", month)
									  .append("year", year)
									  .append("latitude", latitude)
									  .append("longitude", longitude)
									  .append("country", country)
						 			  .append("hour", hour)
									  .append("minute", min);

				// Se inserta el documento en la colección de MongoDB
				collection.insertOne(tweet);
				System.out.println("Tweet numero " + Long.toString(collection.count()));
			}
		};

		FilterQuery fq = new FilterQuery();
		fq.track(keywords.toArray(new String[0]));
		fq.language("es");

		this.twitterStream.addListener(listener);
		this.twitterStream.filter(fq);
	}
	
	public static void main(String[] args) {
		new TwitterStreaming().init();
	}

}
