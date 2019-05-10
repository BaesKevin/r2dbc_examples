package be.kevinbaes.bap.r2dbcshowcase.r2dbc.client;

import be.kevinbaes.bap.r2dbcshowcase.domain.Goal;
import io.r2dbc.client.R2dbc;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;

public class RepositoryClient {
    public static void main(String[] args) {
        new RepositoryClient().run();
    }

    private void run() {
        PostgresqlConnectionConfiguration config = PostgresqlConnectionConfiguration.builder()
                .host("127.0.0.1")
                .port(5432) // optional, defaults to 5432
                .username("postgres")
                .password("postgres")
                .database("goaltracker")
                .build();

        R2dbc r2dbc = new R2dbc(new PostgresqlConnectionFactory(config));

        GoalRepository repo = new GoalRepository(r2dbc);

        System.out.println(repo.deleteAll().blockFirst());
        System.out.println(repo.insert(new Goal(0, "some goal")).blockFirst());
        System.out.println(repo.findAll().collectList().block());
        System.out.println(repo.findById(1).block());



    }
}
