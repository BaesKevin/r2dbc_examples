package be.kevinbaes.bap.r2dbcshowcase.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionUtil {

    public static Connection getConnection() throws SQLException {
        Properties connectionProps = new Properties();
        connectionProps.put("user", "postgres");
        connectionProps.put("password", "postgres");

        return DriverManager.getConnection(
                    "jdbc:postgresql://127.0.0.1:5432/",
                    connectionProps);
    }

}
