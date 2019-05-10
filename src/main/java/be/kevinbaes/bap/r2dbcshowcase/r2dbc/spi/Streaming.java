package be.kevinbaes.bap.r2dbcshowcase.r2dbc.spi;

import be.kevinbaes.bap.r2dbcshowcase.r2dbc.ConnectionUtil;
import be.kevinbaes.bap.r2dbcshowcase.domain.Goal;
import io.r2dbc.spi.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;

/**
 * Demonstrate lazy handling of query results
 */
public class Streaming {

    public static void main(String[] args) throws InterruptedException, IOException {
        new Streaming().run();
    }

    private QueryUtil queryUtil;
    private ConnectionFactory connectionFactory;

    /*********
     * Flux to stream: in the output 'mapping row' and goals interleave, meaning rows are only mapped as needed
     * Default batch size is Integer.MAX_VALUE, it's a good idea to lower it if processing a row is a 'slow' process
     * This is broken for result large resultsets (more than 2500 results starts breaking for me)
     *
     * database session times for 100K goals
     * insert: ~1.5 - 3 seconds
     * select all:  4.5 - 6.5 seconds
     **************/
    private void run() throws InterruptedException, IOException {
        connectionFactory = ConnectionUtil.postgresConnectionFactory();
        queryUtil = new QueryUtil(connectionFactory);

        queryUtil.clearGoalTable();

        // set this to 2750 to break toStream
        // exception is either NumberFormatException or StringIndexOutOfBoundsException
        final int goalCount = 100000;

        Flux<Goal> goalFlux = insertGoals(goalCount)
            .thenMany(selectAllGoals());

        // this breaks for >2.5K results
//        goalFlux.toStream().forEach(System.out::println);

        // the below approaches work for >2.5K results
//        System.out.println(goalFlux.collectList().block().size());
//
        goalFlux.subscribe(
                System.out::println,
                System.err::println,
                () -> System.out.println("complete")
        );

        System.in.read();
    }

    private Long countGoals() {
        return  queryUtil.executeStatement(conn ->
                Mono.from(conn.createStatement("select count(*) from goal;").execute()))
                .flatMap(result -> result.map( (r, rm) -> r.get("count", Long.class)))
                .next().block();
    }

    Flux<Goal> selectAllGoals() {
        return queryUtil.executeStatement(conn ->
                Flux.from(conn.createStatement("select id, name from goal").execute())
                        .flatMap(result -> Flux.from(queryUtil.mapResultToGoal(result)))
        );
    }

    Flux<Goal> selectAllGoalsWithoutUtil() {
        return Mono.from(connectionFactory.create())
                .flatMapMany(conn ->
                        Flux.from(conn.createStatement("select id, name from goal").execute())
                                .concatWith(Mono.from(conn.close()).then(Mono.empty()))
                                .onErrorResume(e -> Mono.from(conn.close()).then(Mono.error(e)))
                                .flatMap(result -> Flux.from(queryUtil.mapResultToGoal(result)))
                );
    }



    /**
     * Insert n goals all as separate insert statements, block after inserting them all
     */
    private Flux<Result> insertGoals(int n) {
        return queryUtil.executeInTransaction(connection -> {
            StringBuilder values = new StringBuilder("insert into goal (name) values ");

            for (int i = 0; i < n - 1; i++) {
                values.append("('goal").append(i).append("'),");
            }

            values.append("('goal").append(n - 1).append("');");

            return connection.createStatement(values.toString()).execute();
        });
    }
}
