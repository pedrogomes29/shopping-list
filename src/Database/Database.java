package Database;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;

import static javax.management.remote.JMXConnectorFactory.connect;

public class Database {

    private final Connection connection;
    public Database(String databaseUrl) throws SQLException {
        databaseUrl = "jdbc:sqlite:"+databaseUrl;
        connection = DriverManager.getConnection(databaseUrl);
        String createTableSQL = "CREATE TABLE IF NOT EXISTS shopping_lists (id INTEGER PRIMARY KEY,hash CHAR(32),shopping_list BLOB)";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(createTableSQL);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void insertData(String hash, byte[] objectBytes) {
        String updateSQL = "INSERT INTO shopping_lists (hash,shopping_list) VALUES (?,?)";

        try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {

            pstmt.setString(1, hash);
            pstmt.setBytes(2, objectBytes);
            pstmt.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public ArrayList<byte[]> getShoppingListsBytes(String startingHash, String endingHash){
        String selectSQL = "SELECT * FROM shopping_list WHERE hash>=? AND hash<=?";
        ArrayList<byte[]> shoppingListsBytes = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, startingHash);
            pstmt.setString(2, endingHash);
            try (ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    byte[] shoppingListBytes = resultSet.getBytes("shopping_list");
                    shoppingListsBytes.add(shoppingListBytes);
                }
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return shoppingListsBytes;
    }
}
