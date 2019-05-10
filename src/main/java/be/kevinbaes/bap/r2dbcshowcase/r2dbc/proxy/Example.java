package be.kevinbaes.bap.r2dbcshowcase.r2dbc.proxy;

import be.kevinbaes.bap.r2dbcshowcase.r2dbc.ConnectionUtil;
import be.kevinbaes.bap.r2dbcshowcase.domain.Goal;
import io.r2dbc.client.Handle;
import io.r2dbc.client.Query;
import io.r2dbc.client.R2dbc;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.io.IOException;

public class Example {
    public static void main(String[] args) throws IOException {
        new Example().run();
    }

    private ConnectionFactory connectionFactory;
    private R2dbc r2dbc;



    private void run() throws IOException {
        connectionFactory = ConnectionUtil.proxyConnectionFactory(ConnectionUtil.H2ConnectionFactory());
//
        r2dbc = new R2dbc(connectionFactory);

        Flux<Integer> goalsInserted = r2dbc.inTransaction(handle -> {
            return insertGoals(handle);
        });


        Flux<Goal> getAllGoals = r2dbc.inTransaction(handle -> {
            Query query = handle.select("select * from goal");

            return query.mapResult(this::mapRowToGoals);
        });

        goalsInserted
            .thenMany(getAllGoals)
            .subscribe(System.out::println, e -> e.printStackTrace(), () -> System.out.println("allGoals complete"));

//        Flux<Goal> allGoals = Mono.from(connectionFactory.create())
//                .flatMapMany(conn ->
//                        Mono.from(conn.beginTransaction())
//                                .then(
//                                        insertGoals(conn)
//                                ) // reference the connection that existed before the transaction
//                                .delayUntil(p -> conn.commitTransaction()) // only emit the result of the insert after the transaction completes
//                                .thenMany(
//
//                                )
//                                .onErrorResume(error -> Mono.from(conn.rollbackTransaction()).then(Mono.error(error))) // on error rollback and propagate error
//                );
//
//        allGoals.subscribe(System.out::println, e -> e.printStackTrace(), () -> System.out.println("allGoals complete"));

        System.in.read();
    }

    private static Publisher<Integer> insertGoals(Handle handle) {
        return handle.execute("INSERT INTO goal (name) VALUES ($1), ($2)", "first goal", "second goal");
    }

    private  Flux<Goal> allGoals() {
        return r2dbc.withHandle(handle ->
                handle.select("select * from goal")
                        .mapRow(row -> {
                            return new Goal(
                                row.get("id", Integer.class),
                                    row.get("name", String.class)
                            );
                        })
        );
    }

    private Publisher<Goal> mapRowToGoals(Result result) {

        return result.map((row, rowMeta) -> {
            System.out.println("mapping row");
            String name = row.get("name", String.class);
            Integer id = row.get("id", Integer.class);

            return new Goal(id, name);
        });
    }


}
