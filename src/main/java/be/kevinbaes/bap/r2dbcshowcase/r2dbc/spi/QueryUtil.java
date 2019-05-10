package be.kevinbaes.bap.r2dbcshowcase.r2dbc.spi;

import be.kevinbaes.bap.r2dbcshowcase.domain.Goal;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * Some methods that make executing queries easier, this is here to show how easy it is to create simple
 * utility methods that take care of opening anc closing connections and transactions.
 */
public class QueryUtil {

    private final ConnectionFactory connectionFactory;

    public QueryUtil(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public Mono<Connection> connect() {
        return Mono.from(connectionFactory.create());
    }

    /**
     * connect a connection and transaction and returns the connection that was used to do it
     */
    public Mono<Connection> beginTransaction() {
        return connect().flatMap(conn ->
                Mono.from(conn.beginTransaction())
                        .then(Mono.just(conn))
        );
    }

    /**
     * Example utility function that can execute any statement in a connection and closes the connection for you.
     * @param statementFunction function that takes a connection and returns a statement to execute
     */
    public <T> Flux<T> executeStatement(Function<Connection, Publisher<? extends T>> statementFunction) {
        return connect()
                .flatMapMany(conn ->
                        Flux.from(statementFunction.apply(conn))
                                .concatWith(Mono.from(conn.close()).then(Mono.empty()))
                                .onErrorResume(e -> Mono.from(conn.close()).then(Mono.error(e)))
                );
    }

    /**
     * Example utility function that can execute any statement in a transaction.
     * Commits the transaction and closes the connection if all went well.
     * Rolls back the transaction and closes the connection on errors.
     * Errors are propagated to the caller.
     * T is the type of result you're expecting back, so can still be used with Result to perform querys where
     * you don't care about the result.
     * @param resultPublisher function that takes a connection and executes a statement
     */
    public <T> Flux<T> executeInTransaction(Function<Connection, Publisher<? extends T>> resultPublisher) {
        return beginTransaction()
                    .flatMapMany( conn ->
                            Flux.from(resultPublisher.apply(conn))
                                    .delayUntil(p -> Mono.from(conn.commitTransaction()))
                                    .concatWith(Mono.from(conn.close()).then(Mono.empty()))
                                    // on a server or client error, try to rollback and then close the connection
                                    .onErrorResume(e -> Mono.from(conn.rollbackTransaction())
                                                    .then(Mono.from(conn.close()))
//                                                  .then(Mono.empty()) // <-- this would swallow the error
                                                    .then(Mono.error(e)) // <-- popagates the error
                                    )
                );
    }

    /**
     * clear the goal table
     */
    public void clearGoalTable() {
        final String deleteGoals = "DELETE FROM goal";

        executeStatement(conn -> Mono.from(conn.createStatement(deleteGoals).execute())
                .flatMap( result ->
                        Mono.from(result.getRowsUpdated())
                ))
                .blockLast();
    }

    Publisher<Goal> mapResultToGoal(Result result){
        return result.map(
                (r, rm) -> {
                    Integer id = r.get("id", Integer.class);
                    System.out.println("mapping row {" + id + "}");
                    return new Goal(
                            id,
                            r.get("name", String.class)
                    );
                }
        );
    }

}
