package cl.citiaps.twitter.streaming.databases;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

//Clase para establecer la conexión con MongoDB
public class MongoConnection {

    private String host;
    private String port;
    private String dbName;
    private MongoClient client;
    private MongoDatabase db;
    private String collName;
    private MongoCollection<Document> collection;

    public MongoConnection(String host, String port, String dbName, String collName){
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.collName = collName;
        createConnection();
    }

    public void createConnection(){
        this.client = new MongoClient(new ServerAddress(this.host, Integer.valueOf(this.port)));
        this.db = client.getDatabase(this.dbName);
    }

    public MongoCollection<Document> getCollection() {
        this.collection = db.getCollection(this.collName);
        return this.collection;
    }
}
