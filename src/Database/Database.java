package Database;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static javax.management.remote.JMXConnectorFactory.connect;

public class Database {

    private final Connection connection;
    public Database(String databaseUrl) throws SQLException {
        databaseUrl = "jdbc:sqlite:"+databaseUrl;
        connection = DriverManager.getConnection(databaseUrl);
        String createTableSQL = "CREATE TABLE IF NOT EXISTS shopping_lists (id TEXT PRIMARY KEY,hash CHAR(32),shopping_list TEXT,shopping_list_hash CHAR(32))";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(createTableSQL);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void insertData(String objectID, String hash, String objectBase64, String objectHash) {
        String updateSQL = "INSERT OR REPLACE INTO shopping_lists (id,hash,shopping_list,shopping_list_hash) VALUES (?, ?, ?,?)";

        try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {

            pstmt.setString(1, objectID);
            pstmt.setString(2, hash);
            pstmt.setString(3, objectBase64);
            pstmt.setString(4, objectHash);
            pstmt.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }


    public String getShoppingList(String objectID) {
        String selectSQL = "SELECT * FROM shopping_lists WHERE id=?";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, objectID);
            try (ResultSet resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString("shopping_list");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public String getShoppingListHash(String objectID) {
        String selectSQL = "SELECT * FROM shopping_lists WHERE id=?";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, objectID);
            try (ResultSet resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString("shopping_list_hash");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public void deleteShoppingLists(String startingHash, String endingHash) {
        String deleteSQL;
        if (startingHash.compareTo(endingHash) < 0)
            deleteSQL = "DELETE FROM shopping_lists WHERE hash>=? AND hash<=?";
        else
            deleteSQL = "DELETE FROM shopping_lists WHERE hash>=? OR hash<=?";

        try (PreparedStatement pstmt = connection.prepareStatement(deleteSQL)) {
            pstmt.setString(1, startingHash);
            pstmt.setString(2, endingHash);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Map<String,String> getShoppingListsBase64(String startingHash, String endingHash){
        String selectSQL;
        if(startingHash.compareTo(endingHash)<0)
            selectSQL = "SELECT * FROM shopping_lists WHERE hash>=? AND hash<=?";
        else
            selectSQL = "SELECT * FROM shopping_lists WHERE hash>=? OR hash<=?";
        HashMap<String,String> shoppingListsBase64 = new HashMap<>();
        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, startingHash);
            pstmt.setString(2, endingHash);
            try (ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    String shoppingListBase64 = resultSet.getString("shopping_list");
                    String shoppingListID = resultSet.getString("id");

                    shoppingListsBase64.put(shoppingListID,shoppingListBase64);
                }
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return shoppingListsBase64;
    }

    public Map<String,String> getShoppingListsHashes(String startingHash, String endingHash){
        String selectSQL;
        if(startingHash.compareTo(endingHash)<0)
            selectSQL = "SELECT * FROM shopping_lists WHERE hash>=? AND hash<=?";
        else
            selectSQL = "SELECT * FROM shopping_lists WHERE hash>=? OR hash<=?";
        HashMap<String,String> shoppingListsToHash = new HashMap<>();
        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, startingHash);
            pstmt.setString(2, endingHash);
            try (ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    String shoppingListHash = resultSet.getString("shopping_list_hash");
                    String shoppingListID = resultSet.getString("id");

                    shoppingListsToHash.put(shoppingListID,shoppingListHash);
                }
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return shoppingListsToHash;
    }
}
