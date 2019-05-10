package be.kevinbaes.bap.r2dbcshowcase.r2dbc.spi.repository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface R2dbcRepository<T, ID> {

    Flux<T> findAll();
    Mono<T> findById(ID id);

    /**
     * Insert or update an element
     * @return the inserted or updated element
     */
    Mono<T> save(T element);

    Mono<Integer> delete(ID id);
    Mono<Integer> deleteAll();
}
