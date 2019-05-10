package be.kevinbaes.bap.r2dbcshowcase.r2dbc.spi;

import be.kevinbaes.bap.r2dbcshowcase.r2dbc.ConnectionUtil;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;

public class ConnectionPool {

    public static void main(String[] args) {

        ConnectionFactory connectionFactory = ConnectionUtil.pooledConnectionFactory();

        Mono<Connection> connectionMono = Mono.from(connectionFactory.create());



    }

}
