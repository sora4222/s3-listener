package com.listener.storable;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.LinkedList;
import java.util.Properties;

import static java.lang.System.exit;

public class SQLiteStorable implements Storable {

    private final static Logger logger = LoggerFactory.getLogger(SQLiteStorable.class.getName());


    private Connection dbConnection;

    public SQLiteStorable(Properties properties) {
        String uri = "";
        if (properties.getProperty("InMemory", "false").equals("true"))
            // Creates a memory database
            uri = "jdbc:sqlite::memory:";
        else
            uri = "jdbc:sqlite:" + properties.getProperty("DataBaseLocation");
        try {
            logger.info("The uri connection string was: " + uri);
            dbConnection = DriverManager.getConnection(uri);

            URI resource = null;
            try {
                resource = this.getClass().getClassLoader().getResource("SQLiteInitialSchema").toURI();
            } catch (NullPointerException exc) {
                logger.error("The SQLiteInitialSchema file doesn't exist and needs to be created.");
                exit(1);
            }
            BufferedReader br = new BufferedReader(
                    new FileReader(
                            new File(
                                    resource
                            )));

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
            logger.info("Schema creation string is:\n" + sb.toString());
            final Statement stmt = dbConnection.createStatement();
            stmt.executeUpdate(sb.toString());


        } catch (URISyntaxException | SQLException | IOException exc) {
            // Will write the exception to log
            logger.error("An error has occured setting up the SQLite storable:\n" + exc.getMessage());
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
            logger.warn("The storable has had an error reading a key\n{}\nSQLState:{}",
                    exc.getMessage(), exc.getSQLState());
            return true;
        }
    }

    /**
     * Will obtain a list of all the filelocations in this database
     * this is intended for TESTING only.
     *
     * @return A {@link LinkedList} containing all of the FILELOCATION
     */
    public LinkedList<String> getKeysWrittenAsList() {
        try {
            PreparedStatement preparedStatement = dbConnection.prepareStatement("SELECT FILELOCATION FROM LISTDATA " +
                    "ORDER BY date(Timestamp) ASC");
            ResultSet resultSet = preparedStatement.executeQuery();
            LinkedList<String> filesToReturn = new LinkedList<>();
            while (resultSet.next()) {
                filesToReturn.add(resultSet.getString(1));
            }
            return filesToReturn;
        } catch (SQLException e) {
            logger.warn(e.getMessage());
            return null;
        }
    }

    /**
     * Gives back the count of the SQLite database.
     * This is for Testing only
     *
     * @return The count as an int of the rows in the database
     */
    public int count() {
        try {
            PreparedStatement countStatement = dbConnection.prepareStatement("SELECT COUNT(*) FROM LISTDATA");
            ResultSet resultSet = countStatement.executeQuery();
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException exc) {
            logger.warn("The count couldn't be acquired");
            return -1;
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
            logger.warn("An error has occured writing a key to the SQLite storable:{},\nSQLiteState:{}",
                    exc.getMessage(), exc.getSQLState());
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
