package be.kevinbaes.bap.r2dbcshowcase.r2dbc.spi.repository;

import be.kevinbaes.bap.r2dbcshowcase.r2dbc.ConnectionUtil;
import be.kevinbaes.bap.r2dbcshowcase.domain.Goal;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import reactor.core.publisher.Mono;

public class CrudOperationsTest {
    private final CrudOperations crudOperations;

    public static void main(String[] args) {
        new CrudOperationsTest().run();
    }

    private void run() {
        Mono<Integer> deleteGoals = crudOperations.delete("delete from goal;");

        Mono<Integer> idMono = deleteGoals.then(
                crudOperations.insert("insert into goal (name) values ($1)", "crud operations")
        );

        Mono<Goal> newGoal = idMono
            .flatMap(id ->
                crudOperations
                    .select("select * from goal where id = $1", this::mapRow, id)
                    .single() // Flux<T> to Mono<T>
            );

        Mono<Goal> updatedGoal = newGoal
            .flatMap(goal ->
                crudOperations
                    .update("update goal set name = $1 where id = $2", "the new name", goal.getId()).single()
                    .then(
                        crudOperations.select("select * from goal where id = $1", this::mapRow, goal.getId()).single()
                    )
            );

        System.out.println(updatedGoal.block());
    }

    private Goal mapRow(Row row, RowMetadata metadata) {
        return new Goal(
            row.get("id", Integer.class),
            row.get("name", String.class)
        );
    }

    public CrudOperationsTest() {
        this.crudOperations = new CrudOperations(ConnectionUtil.postgresConnectionFactory());
    }


}
