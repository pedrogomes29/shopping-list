package Database;

import java.io.File;
import java.sql.*;

import static javax.management.remote.JMXConnectorFactory.connect;

public class Database {

    private final Connection connection;
    public Database(String databaseUrl) throws SQLException {
        databaseUrl = "jdbc:sqlite:"+databaseUrl;
        connection = DriverManager.getConnection(databaseUrl);
        String createTableSQL = "CREATE TABLE IF NOT EXISTS shopping_lists (id INTEGER PRIMARY KEY, shopping_list BLOB)";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(createTableSQL);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void insertData(byte[] objectBytes) {
        String updateSQL = "INSERT INTO shopping_lists (shopping_list) VALUES (?)";

        try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {

            pstmt.setBytes(1, objectBytes);
            pstmt.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
