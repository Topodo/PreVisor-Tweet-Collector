package cl.citiaps.twitter.streaming;

import cl.citiaps.twitter.streaming.databases.MySqlConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.es.SpanishAnalyzer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;


public class SentimentAnalyzer {

    //Se almacenan todos los StopWords que provee el analyzer de Lucene
    private CharArraySet stopWords = new SpanishAnalyzer().getStopwordSet();
    private Map<String, Integer> dictionary;
    private MySqlConnection connection;

    public SentimentAnalyzer(MySqlConnection connection){
        this.connection = connection;
        loadWords();
    }

    private void loadWords() {
        HashSet<String> dictionaryAux = new HashSet<>();
        String[] word;
        int positive;
        int negative;
        int total;
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            dictionaryAux.addAll(IOUtils.readLines(classLoader.getResourceAsStream("LIWCLexicon.csv"), "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.dictionary = new HashMap<>();

        //Se cargan los sentimientos de la palabra
        for(String wordAux : dictionaryAux){
            word = wordAux.split(",");
            positive = Integer.valueOf(word[1]);
            negative = Integer.valueOf(word[2]);
            total = positive - negative;
            //Se verifica si la palabra es un sufijo o una palabra entera
            if(word[0].contains("*")){
                this.dictionary.putIfAbsent(StringUtils.chop(word[0]).trim().toLowerCase(), total);
            } else {
                this.dictionary.putIfAbsent(word[0].toLowerCase(), total);
            }
        }
    }

    //Método que calcula el sentimiento de un tweet
    public void calculateSentiment(String tweet, long tweetId){

        //Tipos de sentimientos
        int negative = 0;
        int positive = 0;
        int neutral = 0;

        int wordsCount = 0;

        //Se tokeniza el tweet
        String[] tokens = tweet.split(" ");
        for(String token : tokens){
            //Se verifica que el token no sea un stopword
            if(!stopWords.contains(StringUtils.stripAccents(token.toLowerCase()))){
                //Se busca la palabra en el diccionario
                if(dictionary.containsKey(StringUtils.stripAccents(token.toLowerCase()))){
                    //Caso en que la palabra sea positiva
                    if(dictionary.get(StringUtils.stripAccents(token.toLowerCase())) > 0){
                        positive++;
                        wordsCount++;
                    } else if(dictionary.get(StringUtils.stripAccents(token.toLowerCase())) < 0){
                        negative++;
                        wordsCount++;
                    } else if(dictionary.get(StringUtils.stripAccents(token.toLowerCase())) == 0){
                        neutral++;
                        wordsCount++;
                    }
                }
            }
        }
        if(wordsCount > 0){
            //Almacena las estadísticas del tweet en MySQL
            this.connection.setEstadisticas(negative, neutral, positive, tweetId);
        }
    }
}
