package cl.citiaps.twitter.streaming.databases;

import java.sql.*;
import java.util.*;

public class MySqlConnection {
    private Connection connection;
    private String username;
    private String pass;
    private String host;
    private String dbName;

    public MySqlConnection(String username, String pass, String host, String dbName) {
        this.username = username;
        this.pass = pass;
        this.host = host;
        this.dbName = dbName;
        createConnection();
    }

    //Método que establece la conexión con MySQL
    public void createConnection() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            this.connection = DriverManager.getConnection(this.host + this.dbName, this.username, this.pass);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    //Método que obtiene todos los keywords almacenados en MySQL
    public Set<String> getKeywords() {
        Set<String> keywords = new HashSet<>();
        String query = "SELECT * FROM keyword";
        try {
            Statement st = this.connection.createStatement();
            ResultSet resultSet = st.executeQuery(query);
            while (resultSet.next()) {
                keywords.add(resultSet.getString("palabra"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return keywords;
    }

    //Método que obtiene los prestadores almacenados en MySQL
    public Map<Integer, String> getPrestadores() {
        Map<Integer, String> prestadores = new HashMap<Integer, String>();
        String query = "SELECT prestador_id, nombre FROM prestador";
        try {
            Statement st = this.connection.createStatement();
            ResultSet resultSet = st.executeQuery(query);
            while (resultSet.next()) {
                prestadores.put(Integer.valueOf(resultSet.getString("prestador_id")), resultSet.getString("nombre"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return prestadores;
    }

    //Método que inserta las estadísticas del tweet en la base de datos MySQL, además de la id del tweet
    public void setEstadisticas(int neg, int neutral, int pos, long tweetId) {
        String query = "INSERT INTO valoracion (negativas, neutras, positivas, tweet_id) " +
                "VALUES (" + neg + "," + neutral + "," + pos + "," + tweetId + ")";
        try {
            Statement st = this.connection.createStatement();
            st.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
