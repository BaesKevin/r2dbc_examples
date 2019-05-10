package be.kevinbaes.bap.r2dbcshowcase.r2dbc.client;

import be.kevinbaes.bap.r2dbcshowcase.domain.Goal;
import io.r2dbc.client.R2dbc;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class GoalRepository {
    private final R2dbc r2dbc;

    public GoalRepository(R2dbc r2dbc) {
        this.r2dbc = r2dbc;
    }

    public Flux<Goal> findAll() {
        return r2dbc.withHandle(handle ->
                handle.select("select * from goal")
                        .mapRow((row, rowMetadata) -> {
                            Integer id = row.get("id", Integer.class);
                            String name = row.get("name", String.class);
                            return new Goal(id, name);
                        }));
    }

    public Mono<Goal> findById(long id) {
        return r2dbc.withHandle(handle ->
                handle.select("select * from goal where id = $1", id)
                        .mapRow((row, rowMetadata) -> {
                            Integer foundId = row.get("id", Integer.class);
                            String name = row.get("name", String.class);
                            return new Goal(foundId, name);
                        }))
                        .next();
    }

    public Flux<Integer> insert(Goal goal) {
        return r2dbc.withHandle(handle ->
                handle.createQuery("INSERT INTO goal (name) VALUES ($1)")
                        .bind("$1", goal.getGoal())
                        .mapResult(Result::getRowsUpdated));
    }

    public Flux<Integer> update(Goal goal) {
        return r2dbc.withHandle(handle ->
                handle.createQuery("UPDATE goal SET name = $1 WHERE id = $2")
                        .bind("$1", goal.getGoal())
                        .bind("$2", goal.getId())
                        .mapResult(Result::getRowsUpdated));
    }

    public Flux<Integer> deleteAll() {
        return r2dbc.withHandle(handle ->
                handle.createQuery("delete from goal")
                        .mapResult(Result::getRowsUpdated));
    }

}
