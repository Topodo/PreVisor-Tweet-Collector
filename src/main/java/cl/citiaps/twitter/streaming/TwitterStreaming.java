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
	private Twitter twitter;

	private TwitterStreaming() {
		this.twitterStream = new TwitterStreamFactory().getInstance();
		this.keywords = new HashSet<>();
		loadKeywords();

		/* Se crea la conexión con MongoDB y se almacena la colección de Tweets */
		this.conn = new MongoConnection("127.0.0.1", 27017, "TBD", "tweets");
		this.collection = this.conn.getCollection();
		twitter = new TwitterFactory().getInstance();
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

				/*
				Se verifica si el status actual es una respuesta de un tweet que fue previamente
				publicado. En caso de que se así, se recupera toda la discusión generada por el
				tweet original
				 */
				/*System.out.println("id_original: " + status.getInReplyToStatusId());
				if(status.getInReplyToStatusId() > 0){
					//Se obtiene la discusión asociada al tweet
					List<Status> retweets = getDiscussion(status, twitter);
					//Geolocalización del retweet
					String countryRetweet;
					String longitudeRetweet;
					String latitudeRetweet;

					//Lista de Hashtags del retweet
					List<String> retweetsHashtags = new ArrayList<>();

					int hRetweetCount;

					for (Status retweet : retweets){
						if(retweet.getId() != status.getId()){
							try {
								countryRetweet = retweet.getUser().getLocation();
							} catch (NullPointerException ne){
								countryRetweet = "none";
							}
							try {
								longitudeRetweet = Double.toString(retweet.getGeoLocation().getLongitude());
								latitudeRetweet = Double.toString(retweet.getGeoLocation().getLatitude());
							} catch (NullPointerException e){
								longitudeRetweet = "none";
								latitudeRetweet = "none";
							}
							hRetweetCount = retweet.getHashtagEntities().length;
							if(hRetweetCount > 0){
								//Se agregan los hashtags al arreglo
								for(TweetEntity hashtag : retweet.getHashtagEntities()){
									retweetsHashtags.add(hashtag.getText());
								}
							}

							// Se crea un documento que representa el JSON que se almacenará en MongoDB
							Document retweetDoc = new Document("id", retweet.getId())
									.append("user", retweet.getUser().getScreenName())
									.append("name", retweet.getUser().getName())
									.append("tweetText", retweet.getText())
									.append("hashtags", retweetsHashtags)
									.append("day", day)
									.append("month", month)
									.append("year", year)
									.append("latitude", latitudeRetweet)
									.append("longitude", longitudeRetweet)
									.append("country", countryRetweet)
									.append("hour", hour)
									.append("minute", min);

							// Se inserta el documento en la colección de MongoDB
							collection.insertOne(retweetDoc);
							System.out.println("Tweet numero " + Long.toString(collection.count()) + " (Es una respuesta)");
						}
					}
				}*/
			}
		};

		FilterQuery fq = new FilterQuery();
		fq.track(keywords.toArray(new String[0]));
		fq.language("es");

		this.twitterStream.addListener(listener);
		this.twitterStream.filter(fq);
	}

	//Método que retorna la discusión generada por un tweet
	public List<Status> getDiscussion(Status status, Twitter twitter){

		List<Status> replies = new ArrayList<>();
		List<Status> tweets;

		try{
			Query query = new Query("to:" + status.getUser().getScreenName());
			query.setSinceId(status.getId());
			QueryResult results;
			do{
				results = twitter.search(query);
				tweets = results.getTweets();
				System.out.println("elementos: " + tweets.size());
				for(Status tweet : tweets){
					System.out.println(tweet.getText() + " lalala");
					//Se compara si la id del tweet original es igual a la id del tweet obtenido por la consulta
					if(status.getId() == tweet.getInReplyToStatusId()){
						replies.add(tweet);
					}
				}
			} while((query = results.nextQuery()) != null);

		} catch (TwitterException e) {
			e.printStackTrace();
		}
		return replies;
	}
	
	public static void main(String[] args) {
		new TwitterStreaming().init();
	}

}
