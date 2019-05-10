package be.kevinbaes.bap.r2dbcshowcase.jdbc;

import be.kevinbaes.bap.r2dbcshowcase.domain.Goal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class BasicQuery {

    public static void main(String[] args) {
        new BasicQuery().run();
    }

    private void run() {

        final String selectGoals = "select id, name from goal";
        List<Goal> goals = new ArrayList<>();

        try (
            Connection connection = ConnectionUtil.getConnection();
            PreparedStatement stmt = connection.prepareStatement(selectGoals)
        ) {
            try(ResultSet resultSet = stmt.executeQuery()) {

                while(resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");

                    goals.add(new Goal(id, name));
                }

            }
        } catch (SQLException e) {
            System.out.println("something went wrong: " + e);
        }

        System.out.println(goals);

    }

}
