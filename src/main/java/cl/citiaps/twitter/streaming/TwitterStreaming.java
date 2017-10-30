package cl.citiaps.twitter.streaming;

import java.io.IOException;
import java.util.*;

import cl.citiaps.twitter.streaming.databases.MongoConnection;
import cl.citiaps.twitter.streaming.databases.MySqlConnection;
import com.mongodb.client.MongoCollection;
import org.apache.commons.io.IOUtils;

import org.bson.Document;
import twitter4j.*;

public class TwitterStreaming {

	private final TwitterStream twitterStream;
	private MongoConnection conn;
	private MongoCollection<Document> collection;
	private Set<String> keywords;
	private Map<Integer, String> prestadores;
	private Twitter twitter;
	private String mongoHost;
	private String mongoPort;
	private String mongoDbName;
	private String mongoCollName;
	private String mysqlHost;
	private String mysqlDbName;
	private String mysqlUsername;
	private String mysqlPassword;
	private Properties prop;
	private MySqlConnection mySqlConnection;

	public TwitterStreaming(Properties dbProperties) {
		this.twitterStream = new TwitterStreamFactory().getInstance();
		this.keywords = new HashSet<>();

		this.prop = dbProperties;
		/*Se cargan las propiedades de las bases de datos*/

		//Propiedades de MongoDB
		this.mongoHost = prop.getProperty("mongo_host");
		this.mongoPort = prop.getProperty("mongo_port");
		this.mongoDbName = prop.getProperty("mongo_db_name");
		this.mongoCollName = prop.getProperty("mongo_coll_name");

		//Propiedades de MySQL
		this.mysqlUsername = prop.getProperty("mysql_username");
		this.mysqlPassword = prop.getProperty("mysql_password");
		this.mysqlHost = prop.getProperty("mysql_host");
		this.mysqlDbName = prop.getProperty("mysql_db_name");

		/* Se crea la conexión con MongoDB y se almacena la colección de Tweets */
		this.conn = new MongoConnection(mongoHost, mongoPort, mongoDbName, mongoCollName);
		this.collection = this.conn.getCollection();
		twitter = new TwitterFactory().getInstance();

		// Se crea la conexión con MySQL para almacenar las estadísticas del tweet y obtener los keywords
		this.mySqlConnection = new MySqlConnection(this.mysqlUsername, this.mysqlPassword, this.mysqlHost, this.mysqlDbName);


		loadKeywords();
		loadPrestadores();
	}

	private void loadKeywords() {
		this.keywords = this.mySqlConnection.getKeywords();
	}

	private void loadPrestadores(){
		this.prestadores = this.mySqlConnection.getPrestadores();
	}

	public void init() {

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

				//Se verifica si el status es un retweet. En caso de serlo, se recupera el tweet original.
				if(status.isRetweet()){
					status = status.getRetweetedStatus();
				}

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

				// Se obtiene la cantidad de menciones del usuario
				UserMentionEntity[] userMentions = status.getUserMentionEntities();
				int userMentionsCount = userMentions.length;

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
						.append("minute", min)
						.append("favouriteCount", status.getFavoriteCount())
						.append("retweetCount", status.getRetweetCount())
						.append("userMentionsCount", userMentionsCount)
						.append("profileImage", status.getUser().getProfileImageURL())
						.append("followersCount", status.getUser().getFollowersCount())
						.append("friendsCount", status.getUser().getFriendsCount())
						.append("userVerified", status.getUser().isVerified())
						.append("createdAt", status.getCreatedAt());

				// Se inserta el documento en la colección de MongoDB
				collection.insertOne(tweet);
				System.out.println("Tweet numero: " + collection.count());
			}
		};

		FilterQuery fq = new FilterQuery();
		fq.track(keywords.toArray(new String[0]));
		fq.language("es");

		this.twitterStream.addListener(listener);
		this.twitterStream.filter(fq);
	}
}
