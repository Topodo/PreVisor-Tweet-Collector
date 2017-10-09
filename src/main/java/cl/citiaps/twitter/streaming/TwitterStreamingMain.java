package cl.citiaps.twitter.streaming;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TwitterStreamingMain {
    //Clase que contiene el método main para ejecutar el Streaming de twitter
    private ClassLoader loader = getClass().getClassLoader();
    public static void main(String[] args){
        //Se obtiene el path del archivo con las propiedades de las bases de datos
        File file = new File(TwitterStreamingMain.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        Properties properties = new Properties();
        try{
            String resourcePath = file.getPath().replace("libs/twitter-streaming-1.0.jar", "") + "resources/main/bd.properties";
            resourcePath = resourcePath.replace("%20", " ");
            InputStream inputStream = new FileInputStream(resourcePath);
            properties.load(inputStream);

            new TwitterStreaming(properties).init();
        } catch(IOException e){
            e.printStackTrace();
        }

    }
}
