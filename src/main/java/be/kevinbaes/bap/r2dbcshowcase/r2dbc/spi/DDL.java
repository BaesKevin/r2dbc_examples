package be.kevinbaes.bap.r2dbcshowcase.r2dbc.spi;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.function.Function;

import static be.kevinbaes.bap.r2dbcshowcase.r2dbc.ConnectionUtil.postgresConnectionFactory;
import static be.kevinbaes.bap.r2dbcshowcase.r2dbc.ConnectionUtil.proxyConnectionFactory;

public class DDL {

    public static void main(String[] args) throws IOException {
        new DDL().run();
    }

    private ConnectionFactory connectionFactory;

    private void run() throws IOException {
        connectionFactory = proxyConnectionFactory(postgresConnectionFactory());

        final String dropTestTable = "drop table if exists test;";
        final String createTestTable = "create table test(\n" +
                "    id int primary key,\n" +
                "    name varchar(50)\n" +
                ");";
        final String badCreate = "create table test(\n" +
                "    id in primary key,\n" +
                "    name varchar(50)\n" +
                ");";

        Mono<Void> createTable = inTransaction((connection -> connection.createStatement(createTestTable)))
                .flatMap(result -> Mono.from(result.getRowsUpdated()))
                .then();
//        Mono<Void> badCreateTable = inTransaction(badCreate);
//        Mono<Void> dropTable = inTransaction(dropTestTable);
        Mono<Long> countRows = rowsInTable("test");

        // drop the table and create the table twice
        Mono<Long> rows = createTable
                .then(countRows);


        rows.subscribe(
                System.out::println, System.err::println, () -> System.out.println("complete")
        );

        System.in.read();
    }

    private Mono<Connection> openConnection() {
        return Mono.from(connectionFactory.create());
    }
    private Mono<Void> beginTransaction(Connection connection) { return Mono.from(connection.beginTransaction()); }
    private Mono<Void> commitTransaction(Connection connection) { return Mono.from(connection.commitTransaction()); }
    private Mono<Void> rollbackTransaction(Connection connection) { return Mono.from(connection.rollbackTransaction()); }

    private Flux<Result> executeStatement(Statement statement) {
        return Flux.from(statement.execute());
    }

    /**
     * Run any statement that doesn't return results in a transaction.
     * Returns a completion signal or propagates an error if any error happens.
     * Rolls back the transaction on error.
     */
    private Flux<Result> inTransaction(Function<Connection, Statement> statementCreator) {
        return openConnection()
                .flatMapMany(
                        conn ->
                                beginTransaction(conn)
                                        // when beginTransaction completes, execute the DDL
                                        .thenMany(
                                                executeStatement(statementCreator.apply(conn)).onErrorResume(
                                                        e -> rollbackTransaction(conn)
                                                                .doOnRequest(l -> System.out.println("rolling back"))
                                                                .then(Mono.error(e))
                                                )
                                        )
                                        // standard transaction bookkeeping:

                                        .delayUntil(
                                                p -> commitTransaction(conn)
                                        )

                );
    }

    /**
     * Execute any statement without results, creates a new connection
     */
    private Mono<Void> execute(String sql) {
        return Mono.from(connectionFactory.create())
                .flatMap(
                        conn ->
                                Mono.from(conn
                                        .createStatement(sql)
                                        .execute())
                                        .flatMap(
                                                result -> Mono.from(result.getRowsUpdated())
                                                        .thenEmpty(Mono.empty())
                                        )

                );
    }

    /**
     * Run multiple statements with the same connection
     */
    private Mono<Void> execute(Connection conn, String... querys) {
        Flux<Void> allQuerys = Flux.empty();

        for(String sql : querys)
        allQuerys = allQuerys.concatWith(Mono.from(conn
                .createStatement(sql)
                .execute())
                .flatMap(
                        result -> Mono.from(result.getRowsUpdated())
                                .thenEmpty(Mono.empty())
                ));


        return allQuerys.thenEmpty(Mono.empty());
    }



    private Mono<Long> rowsInTable(String tableName) {
        return Mono.from(connectionFactory.create())
                .flatMapMany(
                        conn ->
                                Mono.from(conn.createStatement("select count(*) from " + tableName).execute())
                                        .flatMapMany(result ->
                                            result.map((row, rm) -> row.get("count", Long.class)))
                ).next();
    }

}
