package be.kevinbaes.bap.r2dbcshowcase.r2dbc.spi.repository;

import be.kevinbaes.bap.r2dbcshowcase.r2dbc.ConnectionUtil;
import be.kevinbaes.bap.r2dbcshowcase.domain.Goal;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class GoalRepository implements R2dbcRepository<Goal, Integer>{

    private final CrudOperations crudOperations;

    public GoalRepository() {
        this.crudOperations = new CrudOperations(ConnectionUtil.postgresConnectionFactory());
    }

    @Override
    public Flux<Goal> findAll() {
        return crudOperations.select("select * from goal", this::mapRow);
    }

    @Override
    public Mono<Goal> findById(Integer id) {
        return crudOperations.select("select * from goal where id = $1", this::mapRow, id).next();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Mono<Goal> save(Goal element) {
        return findById(element.getId())
            .flatMap(
                existingGoal -> crudOperations
                    .update("update goal set name = $1 where id = $2", element.getGoal(), existingGoal.getId())
                    .flatMap(rowsUpdated -> findById(existingGoal.getId()))
            )
            .switchIfEmpty(
                crudOperations
                    .insert("insert into goal (name) values ($1)", element.getGoal())
                    .flatMap(id -> findById(Integer.valueOf(id)))
            );
    }

    @Override
    public Mono<Integer> delete(Integer integer) {
        return null;
    }

    @Override
    public Mono<Integer> deleteAll() {
        return crudOperations.delete("delete from goal;");
    }

    private Goal mapRow(Row row, RowMetadata metadata) {
        return new Goal(
                row.get("id", Integer.class),
                row.get("name", String.class)
        );
    }
}
