package com.Jesse.storable;


import com.Jesse.Storable;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

import javax.xml.ws.WebServiceException;
import java.util.Properties;

public class SQLLiteStorable extends Storable {

    private Connection dbConnection;

    public SQLLiteStorable(Properties properties) {
        String uri = "";
        if (properties.getProperty("InMemory", "false").equals("true"))
            // Creates a memory database
            uri = "jdbc:sqlite::memory:";
        else
            uri = "jdbc:sqlite:" + properties.getProperty("DataBaseLocation");
        try {
            dbConnection = DriverManager.getConnection(uri);

            // Check if the table exists
            DatabaseMetaData metaData = dbConnection.getMetaData();
            ResultSet result = metaData.getTables(null,
                    null, "LISTDATA",
                    new String[]{"TABLE"});

            BufferedReader br = new BufferedReader(
                    new FileReader("SQLiteInitialSchema"));

            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            // Reads all the lines into the string
            while (line != null) {
                sb.append(line);
                sb.append("\n");
                line = br.readLine();
            }

            final Statement stmt = dbConnection.createStatement();
            stmt.executeUpdate(sb.toString());


        } catch (SQLException | IOException exc) {
            // Will write the exception to log
            dbConnection = null;
        }
    }

    /**
     * Checks whether the key is in the storable.
     *
     * @param key the key as a string
     * @return returns as a boolean whether the key is within the storable
     */
    @Override
    public boolean keyAlreadyRead(String key) {
        return false;
    }

    /**
     * Attempts to put the key in the storable
     *
     * @param key the key as a string
     * @return returns a boolean whether the key was written successfully or not
     */
    @Override
    public boolean putKey(String key) {
        return false;
    }

    @Override
    public Iterable<String> keysIterable() {
        return null;
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() throws WebServiceException {

    }
}
