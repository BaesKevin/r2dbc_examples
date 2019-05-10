package be.kevinbaes.bap.r2dbcshowcase.r2dbc.spi.repository;

import be.kevinbaes.bap.r2dbcshowcase.r2dbc.spi.QueryUtil;
import io.r2dbc.spi.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

public class CrudOperations {

    private final QueryUtil queryUtil;

    public CrudOperations(ConnectionFactory connectionFactory) {
        queryUtil = new QueryUtil(connectionFactory);
    }

    /**
     * Execute a select statement with 0 or more parameters to bind.
     * @param sql statement to execute
     * @param rowMapper BiFunction<Row,RowMetadata, T> to convert a single row to a T
     * @param params parameters to bind to the SQL statement
     * @param <T> the type returned by the rowmapper
     * @return Flux<T>
     */
    public <T> Flux<T> select(String sql, BiFunction<Row, RowMetadata, T> rowMapper, Object... params) {
        return queryUtil.executeStatement(conn -> {
            Statement stmt = createStatementWithParams(conn, sql, params);

            return Flux.from(stmt.execute()).flatMap(result -> result.map(rowMapper));
        });
    }

    /**
     * insert a row and return the generated id
     */
    public Mono<Integer> insert(String sql, Object... params) {
        return queryUtil.executeStatement(conn ->{
            Statement stmt = createStatementWithParams(conn, sql, params);
            stmt.returnGeneratedValues("id");

            return Flux.from(stmt.execute()).flatMap(result ->
                    Mono.from(result.map((row, rm) -> row.get("id", Integer.class)))
            );
        }).single();
    }

    /**
     * update record(s) and return the number of updated rows.
     */
    public Mono<Integer> update(String sql, Object... params) {
        return rowsUpdatedStatement(sql, params);
    }

    /**
     * delete record(s) and return the number of updated rows.
     */
    public Mono<Integer> delete(String sql, Object... params) {
        return rowsUpdatedStatement(sql, params);
    }

    private Mono<Integer> rowsUpdatedStatement(String sql, Object... params) {
        return queryUtil.executeStatement(conn ->{
            Statement stmt = createStatementWithParams(conn, sql, params);

            return Flux.from(stmt.execute()).flatMap(result ->
                    Mono.from(result.getRowsUpdated())
            );
        }).single();
    }

    // TODO check if param is null to use bindNull
    private Statement createStatementWithParams(Connection connection, String sql, Object... params) {
        Statement stmt = connection.createStatement(sql);

        for(int i = 0; i < params.length; i++) {
            Object param = params[i];

            if(param == null) {
                stmt.bindNull(i, Object.class);
            } else {
                stmt.bind(i, params[i]);
            }
        }

        return stmt;
    }
}
