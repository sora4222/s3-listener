package com.Jesse.storable;


import com.Jesse.Storable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

import java.util.Properties;

public class SQLLiteStorable implements Storable {

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

            // Ensures progress
            dbConnection.setAutoCommit(true);

            // Creates the table if it doesn't exist
            final Statement stmt = dbConnection.createStatement();
            stmt.executeUpdate(sb.toString());


        } catch (SQLException | IOException exc) {
            // Will write the exception to log
            dbConnection = null;
        }
    }

    /**
     * Checks whether a key is in the storable.
     *
     * @param key the key as a string
     * @return returns as a boolean whether the key is within the storable
     */
    @Override
    public boolean keyAlreadyRead(String key) {
        try {
            // Creates the prepared statement
            PreparedStatement selectStatement =
                    dbConnection.prepareStatement("SELECT * FROM LISTDATA WHERE FILELOCATION=?");
            selectStatement.setString(1, key);
            ResultSet result = selectStatement.executeQuery();

            // Returns the result
            return result.next();
        }
        catch (SQLException exc){
            // Ensures the listener won't output everything to this database in the case
            // of a failure.
            return true;
        }
    }

    /**
     * Attempts to put the key in the storable
     *
     * @param key the key as a string
     * @return returns a boolean whether the key was written successfully or not
     */
    @Override
    public boolean putKey(String key) {
        try {
            PreparedStatement updateStatement =
                    dbConnection.prepareStatement("INSERT INTO LISTDATA(FILELOCATION) VALUES(?)");
            updateStatement.setString(1, key);
            updateStatement.execute();
        }
        catch (SQLException exc){
            return false;
        }
        return true;
    }

    /**
     * Closes the connection for the SQLite database.
     */
    @Override
    public void close(){
        try {
            dbConnection.close();
        }catch (SQLException exc){
            return;
        }
    }
}
