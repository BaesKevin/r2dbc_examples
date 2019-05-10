package be.kevinbaes.bap.r2dbcshowcase.r2dbc.spi;

import be.kevinbaes.bap.r2dbcshowcase.r2dbc.ConnectionUtil;
import be.kevinbaes.bap.r2dbcshowcase.domain.Goal;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * Select, insert, delete and batch examples
 */
public class BasicQuery {

    public static void main(String[] args) {
        new BasicQuery().run();
    }

    private ConnectionFactory connectionFactory;

    private void run() {

        /***************
         * set up the connection factory, use convenience method from ConnectionUtil class in other examples
         ***************/
        PostgresqlConnectionConfiguration config = PostgresqlConnectionConfiguration.builder()
                .host("127.0.0.1")
                .port(5432) // optional, defaults to 5432
                .username("postgres")
                .password("postgres")
                .database("postgres") // default postgres db name is 'postgres'
                .build();

//        connectionFactory = new PostgresqlConnectionFactory(config);
        connectionFactory = ConnectionUtil.discover();

        // all methods on SPI types return <? extends Publisher> to be compatible with other reactive libs
        // this tends to result in tedious code if using SPI directly because of many .from calls
        QueryUtil queryUtil = new QueryUtil(connectionFactory);

        /********************
         *  INSERT (see select example for full explanation of what all of this does)
         ********************/

        queryUtil.clearGoalTable();

        Flux<Integer> insertTwoGoals = Mono.from(connectionFactory.create())
                .flatMapMany(conn ->
                        Flux.from(conn.createStatement("INSERT INTO goal (name) VALUES ($1),($2)")
                                .bind("$1", "Create an application")
                                .bind("$2", "Create a second application")
                                .execute())
                                .flatMap(result -> Mono.from(result.getRowsUpdated()))
                                .concatWith(Mono.from(conn.close()).then(Mono.empty()))
                                .onErrorResume(e -> Mono.from(conn.close()).then(Mono.error(e)))
                );

        System.out.println("num goals inserted: " + insertTwoGoals.collectList().block());

        /*******************
         * SELECT: MAP ROWS TO DOMAIN OBJECTS
         ******************/

        // Preparing and executing the query
        //
        // 1. get a connection
        // 2. flatMapMany because we map a single connection to multiple results
        // 3. Create a Statement
        // 4. Execute the statement. This returns a Publisher<Result> which we need to convert to Flux<Result>
        // 5. add an empty Mono to the end of the result flux that closes the connection
        // 6. in case of error, subscribe to a Mono that closes the connection and propagates the error
        //
        // The result is a sequence that emits 0 to N Result objects and always closes the connection when complete.
        Flux<Result> goalNamesResultFlux = Mono.from(connectionFactory.create()) // 1
            .flatMapMany( //2.
                conn -> Flux.from(
                            conn
                                .createStatement("SELECT id, name FROM goal") // 3.
                                .execute() //4.
                        )
                        // 4.5.
                        .concatWith(Mono.from(conn.close()).then(Mono.empty())) // 5
                        .onErrorResume(e -> Mono.from(conn.close()).then(Mono.error(e))) // 6
            );

        // Consume the results and create a Goal for each result
        // 1. Convert every value emitted by the Result flux using Result#map.
        //    The map function is NOT called when there are no results
        // 2. For every Result the BiFunction passed to the map function is called
        //    rowMetadata contains the java type for the columns, useful for type mapping issues
        //    because the error when trying to map to a wrong type is "Cannot decode value of type SomeClass
        //
        // Note that consuming results can also happen on 4.5.
        Flux<Goal> goalNamesFlux = goalNamesResultFlux
            .flatMap(
                result -> result.map( // 1.
                    (row, rowMetadata) -> { // 2.
                        Integer id = row.get("id", Integer.class);
                        String name = row.get("name", String.class);

                        return new Goal(id, name);
                    })
            );

        System.out.println("Selected goals: " + goalNamesFlux.collectList().block());


        /******************
         * DELETE
         ******************/

        Flux<Result> deleteGoals = Mono.from(connectionFactory.create())
            .flatMapMany(conn ->
                Flux.from(conn.createStatement("DELETE FROM goal").execute())
                    .concatWith(Mono.from(conn.close()).then(Mono.empty()))
                    .onErrorResume(e -> Mono.from(conn.close()).then(Mono.error(e)))
            );

        // optionally do something with  result, like counting how many rows were deleted
        Flux<Integer> numDeletedGoals = deleteGoals.flatMap(result -> Mono.from(result.getRowsUpdated()));

        // go back to blocking behavior since this is a demo application
        System.out.println("goals deleted: " + numDeletedGoals.collectList().block());
    }


    /******************
     * MULTIPLE SEPARATE INSERT STATEMENTS
     ******************/
    private void multipleInserts() {
        // using .add on a statement will result in 2 separate statements being executed
        // consequence: getRowsUpdated returns 1 as it is getting the result from 1 of the statements
        Flux<Integer> multipleInsertStatement = Mono.from(connectionFactory.create())
                .flatMapMany(conn ->
                        Flux.from(conn
                                .createStatement("INSERT INTO goal (name) VALUES ($1)")
                                .bind("$1", "Create an application")
                                .add() // <---- creates a new binding which will result in 2 separate queries
                                .bind("$1", "Create a second application")
                                .execute())
                                .flatMap(result -> Mono.from(result.getRowsUpdated()))
                                .concatWith(Mono.from(conn.close()).then(Mono.empty()))
                                .onErrorResume(e -> Mono.from(conn.close()).then(Mono.error(e)))
                );

        System.out.println("multiple separate inserts rows updated: " + multipleInsertStatement.collectList().block());
    }

    /****************
     * BATCH INSERT WITH THE UTILITY FUNCTION
     * The result of each query is signalled individually, so we have to reduce it to a single value
     *
     * Logs to prove it works (see readme to learn how to enable this logging on Postgres):
     * 2019-05-04 08:14:54.180 UTC [300] LOG:  connection received: host=172.17.0.1 port=46662
     * 2019-05-04 08:14:54.181 UTC [300] LOG:  connection authorized: user=postgres database=goaltracker
     * 2019-05-04 08:14:54.187 UTC [300] LOG:  statement: INSERT INTO goal (name) VALUES ('goal0'); INSERT INTO goal (name) VALUES ('goal1'); INSERT INTO goal (name) VALUES ('goal2'); INSERT INTO goal (name) VALUES ('goal3'); INSERT INTO goal (name) VALUES ('goal4'); INSERT INTO goal (name) VALUES ('goal5'); INSERT INTO goal (name) VALUES ('goal6'); INSERT INTO goal (name) VALUES ('goal7'); INSERT INTO goal (name) VALUES ('goal8'); INSERT INTO goal (name) VALUES ('goal9')
     * 2019-05-04 08:14:54.239 UTC [300] LOG:  disconnection: session time: 0:00:00.059 user=postgres database=goaltracker host=172.17.0.1 port=46662
     ****************/
    private void batchInsert() {
        Mono<Integer> rowsUpdatedAfterBatchInsert = executeStatement( (connection) -> createInsertBatch(connection).execute())
                .flatMap(result -> Mono.from(result.getRowsUpdated()))
                .reduce(0, (total, singleQueryRowsUpdated) -> {
                    return total + singleQueryRowsUpdated;
                });

        System.out.println("rows updated after batch: " + rowsUpdatedAfterBatchInsert.block());

    }

    /**
     * create a batch of ten insert querys
     */
    private Batch createInsertBatch(Connection connection) {
        Batch batch = connection.createBatch();

        for (int i = 0; i < 10; i++) {
            batch.add("INSERT INTO goal (name) VALUES ('goal" + i + "')");
        }

        return batch;
    }

    /**
     * Example function that can execute any statement in a connection and closes the connection for you
     * @param statementFunction function that takes a connection and returns a statement to execute
     */
    private <T> Flux<T> executeStatement(Function<Connection, Publisher<? extends T>> statementFunction) {
        return Mono.from(connectionFactory.create())
            .flatMapMany(conn ->
                    Flux.from(statementFunction.apply(conn))
                        .concatWith(Mono.from(conn.close()).then(Mono.empty()))
                        .onErrorResume(e -> Mono.from(conn.close()).then(Mono.error(e)))
            );
    }

}
