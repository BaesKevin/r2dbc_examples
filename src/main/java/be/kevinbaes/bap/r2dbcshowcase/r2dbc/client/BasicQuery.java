package be.kevinbaes.bap.r2dbcshowcase.r2dbc.client;

import be.kevinbaes.bap.r2dbcshowcase.domain.Goal;
import io.r2dbc.client.Handle;
import io.r2dbc.client.Query;
import io.r2dbc.client.R2dbc;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class BasicQuery {

    public static void main(String[] args) {
        new BasicQuery().run();
    }

    private void run() {

        /*************
         * CREATE R2DBC INSTANCE
         **************/

        PostgresqlConnectionConfiguration config = PostgresqlConnectionConfiguration.builder()
                .host("127.0.0.1")
                .port(5432) // optional, defaults to 5432
                .username("postgres")
                .password("postgres")
                .database("goaltracker")
                .build();

        R2dbc r2dbc = new R2dbc(new PostgresqlConnectionFactory(config));

        // each new subscription creates a new connection
        Mono<Handle> handleMono = r2dbc.open();

        /******************
         * DELETE
         ******************/

        Flux<Integer> deleteAllRows = r2dbc.withHandle(handle ->
                handle.createQuery("delete from goal")
                        .mapResult(Result::getRowsUpdated));

        System.out.println("delete rows: " + deleteAllRows.blockFirst());

        /********************
         *  INSERT
         ********************/

        // withHandle closes the connection for us, R2DBC class convenience methods will tend to do that
        // methods on Handle won't
        Flux<Integer> rowsUpdatedFlux = handleMono
                .flatMapMany(handle ->
                                handle.createQuery("INSERT INTO goal (name) VALUES ($1)")
                                        .bind("$1", "first")
                                        .mapResult(Result::getRowsUpdated)
                                        // identical lines
                                        .concatWith(Mono.from(handle.close()).then(Mono.empty()))
//                                    .concatWith(ReactiveUtils.typeSafe(handle::close))
                );

        System.out.println("rows inserted: " + rowsUpdatedFlux.blockFirst());

        // withHandle calls connect(), flatMapMany on the handle and closes the connection
        Flux<Integer> rowsUpdated = r2dbc.withHandle(handle ->
                handle.createQuery("INSERT INTO goal (name) VALUES ($1)")
                        .bind("$1", "first")
                        .mapResult(Result::getRowsUpdated));

        System.out.println("rows inserted: " + rowsUpdated.blockFirst());

        /*******************
         * SELECT
         ******************/

        Flux<Goal> goals = r2dbc.withHandle(handle -> {
            // build whatever complex query, select has varargs for positional parameter binding
            Query query = handle.select("select * from goal");

            return query.mapRow((row, rowMetadata) -> {
                Integer id = row.get("id", Integer.class);
                String name = row.get("name", String.class);
                return new Goal(id, name);
            });
        });

        System.out.println("goals: " + goals.collectList().block());

        /****************
         * MULTIPLE SEPARATE STATEMENTS
         *****************/


        Flux<Goal> allGoals = r2dbc.withHandle(handle ->
                handle.select("select * from goal")
                        .mapRow((row, rowMetadata) -> {
                            Integer id = row.get("id", Integer.class);
                            String name = row.get("name", String.class);
                            return new Goal(id, name);
                        }));

        System.out.println("all goals followd by delete");
        allGoals
                .map(goal -> {
                    System.out.println(goal);
                    return goal; // propagate the result
                })
                .thenMany( deleteAllRows.map(i -> {
                    System.out.println("deleted " + i);
                    return i;  // propagate the result
                }))
                .blockLast();

    }
}
