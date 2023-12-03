package Database;

import java.sql.*;
import java.util.ArrayList;


public class Database implements AutoCloseable{

    private final Connection connection;
    public Database(String databaseUrl) throws SQLException {
        databaseUrl = "jdbc:sqlite:"+databaseUrl;
        connection = DriverManager.getConnection(databaseUrl);
        String createTableSQL = "CREATE TABLE IF NOT EXISTS shopping_lists (id TEXT PRIMARY KEY,hash CHAR(32),shopping_list TEXT)";

        Statement statement = connection.createStatement();
        statement.executeUpdate(createTableSQL);

    }

    public void insertData(String objectID, String hash, String objectBase64) throws SQLException {
        String updateSQL = "INSERT OR REPLACE INTO shopping_lists (id,hash,shopping_list) VALUES (?, ?, ?)";

        PreparedStatement pstmt = connection.prepareStatement(updateSQL);

        pstmt.setString(1, objectID);
        pstmt.setString(2, hash);
        pstmt.setString(3, objectBase64);
        pstmt.executeUpdate();

    }

    public String getShoppingList(String objectID) throws SQLException {
        String selectSQL = "SELECT * FROM shopping_lists WHERE id=?";
        PreparedStatement pstmt = connection.prepareStatement(selectSQL);
        pstmt.setString(1, objectID);

        try(ResultSet resultSet = pstmt.executeQuery()) {
            if (resultSet.next())
                return resultSet.getString("shopping_list");

        }catch (SQLException exception){
            return "null";
        }

        return null;
    }

    public ArrayList<String> getShoppingListsBytes(String startingHash, String endingHash) {
        String selectSQL = "SELECT * FROM shopping_lists WHERE hash>=? AND hash<=?";
        ArrayList<String> shoppingListsBase64 = new ArrayList<>();
        try(PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, startingHash);
            pstmt.setString(2, endingHash);

            try(ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    String shoppingListBase64 = resultSet.getString("shopping_list");
                    shoppingListsBase64.add(shoppingListBase64);
                }
                return shoppingListsBase64;
            }catch (SQLException sqlException){
                return null;
            }

        }catch (SQLException sqlException) {
            System.err.println("Warning: getShoppingLists with id betweem " + startingHash + " and " + endingHash);
            return null;
        }
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
