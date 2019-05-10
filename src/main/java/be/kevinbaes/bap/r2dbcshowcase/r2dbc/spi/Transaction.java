package be.kevinbaes.bap.r2dbcshowcase.r2dbc.spi;

import be.kevinbaes.bap.r2dbcshowcase.r2dbc.ConnectionUtil;
import be.kevinbaes.bap.r2dbcshowcase.domain.Goal;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;

/**
 * Playing with transactions. Every example contains my log output to prove that connections transactions are
 * managed correctly
 */
public class Transaction {

    static ConnectionFactory connectionFactory;

    static QueryUtil queryUtil;

    public static void main(String[] args) throws IOException {
        connectionFactory = ConnectionUtil.postgresConnectionFactory();
        connectionFactory = ConnectionUtil.proxyConnectionFactory(connectionFactory);

        queryUtil = new QueryUtil(connectionFactory);

        createTableAndRollback();
        rollbackOnServerError();
        rollbackOnClientError();
    }


    /**********
     * TRANSACTION WITH ONE INSERT STATEMENT
     *
     * log output:
     * 2019-05-04 06:53:49.391 UTC [68] LOG:  connection received: host=172.17.0.1 port=46522
     * 2019-05-04 06:53:49.513 UTC [68] LOG:  connection authorized: user=postgres database=goaltracker
     * 2019-05-04 06:53:49.568 UTC [68] LOG:  statement: BEGIN
     * 2019-05-04 06:53:49.575 UTC [68] LOG:  statement: create table sometable( );
     * 2019-05-04 06:53:49.598 UTC [68] LOG:  statement: ROLLBACK
     * 2019-05-04 06:53:49.608 UTC [68] LOG:  disconnection: session time: 0:00:00.217 user=postgres database=goaltracker host=172.17.0.1 port=46522
     * 2019-05-04 06:53:49.615 UTC [69] LOG:  connection received: host=172.17.0.1 port=46524
     * 2019-05-04 06:53:49.617 UTC [69] LOG:  connection authorized: user=postgres database=goaltracker
     * 2019-05-04 06:53:49.627 UTC [69] LOG:  statement: BEGIN
     * 2019-05-04 06:53:49.629 UTC [69] LOG:  statement: create table sometable( );
     * 2019-05-04 06:53:49.633 UTC [69] LOG:  statement: ROLLBACK
     * 2019-05-04 06:53:49.637 UTC [69] LOG:  disconnection: session time: 0:00:00.021 user=postgres database=goaltracker host=172.17.0.1 port=46524
     **********/
    private static void createTableAndRollback() {
        // note: Mono::then takes a Mono, not a function that returns a mono
        // error 'target type of a lambda expression must be a functional interface means
        // you are trying to assign a function interface to something that's not a functional interface
        // in the 'then' case: a Function<Mono<T>> to a Mono<T>

        Flux<Result> createSomeTable = Mono.from(connectionFactory.create())
                .flatMapMany(conn ->
                        Mono.from(conn.beginTransaction())
                                // thenMany because a single statement can contain multiple sql statements and thus
                                // return multiple results
                                .thenMany(
                                        Mono.from(
                                                conn.createStatement("create table sometable( );")
                                                        .execute())
                                ) // reference the connection that existed before the transaction
                                // roll back the transaction ourselves
                                .delayUntil(p -> Mono.from(conn.rollbackTransaction()))
                                // connection bookkeeping
                                .concatWith(Mono.from(conn.close()).then(Mono.empty()))
                                .onErrorResume(e -> Mono.from(conn.close()).then(Mono.empty()))
                );

        createSomeTable.collectList().block();
        createSomeTable.collectList().block();
    }

    /**
     * Do an insert that generates a serverside error.
     * First call result.map or result.getRowsUpdated, then chain
     * onErrorResume and delayUntil (these two can be switched).
     * <p>
     *
     * Log output:
     * 2019-05-04 06:53:49.642 UTC [70] LOG:  connection received: host=172.17.0.1 port=46526
     * 2019-05-04 06:53:49.643 UTC [70] LOG:  connection authorized: user=postgres database=goaltracker
     * 2019-05-04 06:53:49.651 UTC [70] LOG:  statement: BEGIN
     * 2019-05-04 06:53:49.655 UTC [70] LOG:  statement: insert into goal (id, name) values (1, 'wow');
     * 2019-05-04 06:53:49.655 UTC [70] ERROR:  duplicate key value violates unique constraint "goal_pkey"
     * 2019-05-04 06:53:49.655 UTC [70] DETAIL:  Key (id)=(1) already exists.
     * 2019-05-04 06:53:49.655 UTC [70] STATEMENT:  insert into goal (id, name) values (1, 'wow');
     * 2019-05-04 06:53:49.678 UTC [70] LOG:  statement: ROLLBACK
     * 2019-05-04 06:53:49.681 UTC [70] LOG:  disconnection: session time: 0:00:00.038 user=postgres database=goaltracker host=172.17.0.1 port=46526
     */
    private static void rollbackOnServerError() {
        Flux<Integer> getGoals = Mono.from(connectionFactory.create())
                .flatMapMany(conn ->
                        Mono.from(conn.beginTransaction())
                                .thenMany(
                                        Mono.from(
                                                conn.createStatement(
                                                        "insert into goal (id, name) values (1, 'wow');"
                                                ).execute())
                                                .doOnError(e -> System.out.println("execute error"))
                                )
                                .flatMap(result -> Mono.from(result.getRowsUpdated()))
                                .onErrorResume(error -> {
                                    System.out.println("Error happened");
                                    return Mono.from(conn.rollbackTransaction())
                                            .then(Mono.from(conn.close()))
                                            .then(Mono.error(error));
                                })
                                .delayUntil(p -> Mono.from(conn.commitTransaction()))
                                // connection bookkeeping
                                .concatWith(Mono.from(conn.close()).then(Mono.empty()))
                );


        try {
            getGoals.collectList().block();
        } catch (Exception e) {
            System.out.println("insert goals error: " + e.getMessage());
        }
    }

    /**
     * a bit of a nonsene example to roll back on client error, but just to show that it is possible
     * Uses the executeInTransaction utility function to show flexibility
     * <p>
     * 2019-05-04 06:53:49.692 UTC [71] LOG:  connection received: host=172.17.0.1 port=46528
     * 2019-05-04 06:53:49.694 UTC [71] LOG:  connection authorized: user=postgres database=goaltracker
     * 2019-05-04 06:53:49.702 UTC [71] LOG:  statement: BEGIN
     * 2019-05-04 06:53:49.705 UTC [71] LOG:  statement: select * from goal;
     * 2019-05-04 06:53:49.727 UTC [71] LOG:  statement: ROLLBACK
     * 2019-05-04 06:53:49.730 UTC [71] LOG:  disconnection: session time: 0:00:00.038 user=postgres database=goaltracker host=172.17.0.1 port=46528
     */
    private static void rollbackOnClientError() {
        Flux<Goal> getGoals = queryUtil.executeInTransaction(connection ->
                    Flux.from(connection.createStatement("select * from goal;").execute())
                            .flatMap(result -> result.map((row, metadata) -> {

                                // name is not an Integer, this throws a Netty exception
                                row.get("name", Integer.class);

                                System.out.println("mapping row");
                                return new Goal(
                                        row.get("id", Integer.class),
                                        row.get("name", String.class)
                                );
                            }))
                );


        try {
            getGoals.collectList().block();
        } catch (Exception e) {
            System.out.println("get goals error: " + e.getMessage());
        }
    }


}
