package be.kevinbaes.bap.r2dbcshowcase.r2dbc;

import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.proxy.ProxyConnectionFactory;
import io.r2dbc.proxy.support.QueryExecutionInfoFormatter;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class ConnectionUtil {

    /**
     * Basic explanation of how R2DBC driver discovery works:
     *
     * 1. The r2dbc spi defines has a ConnectionFactoryProvider service interface which should be implemented by
     *    drivers if they want to be discovered.
     *    ConnectionFactories#get takes configuration options and tries to find a driver which supports them.
     *    ConnectionFactory#supports returns true if it supports the ConnectionFactoryOptions.
     *
     * 2. io.r2dbc.spi.ConnectionFactories#loadProviders uses java.util.ServiceLoader to look for avaialable drivers of type
     *    ConnectionFactoryProvider. The ServiceLoader looks for a file named META-INF/services/io.r2dbc.spi.ConnectionFactoryProvider.
     *    The content of this file is the fully qualified name of the driver-specific ConnectionFactoryProvider,
     *    e.g. io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider
     *
     *  The advantage of this is that the "postgres" string might come from the environment, i.e. configuration file
     *  or environment variable to change the used driver without having to recompile.
     *
     *  Sources:
     *  java.util.ServiceProvider javadoc
     *  https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html
     *
     *  r2dbc SPI
     *  https://github.com/r2dbc/r2dbc-spi/blob/master/r2dbc-spi/src/main/java/io/r2dbc/spi/ConnectionFactories.java#L119
     *
     *  Postgres driver
     *  https://github.com/r2dbc/r2dbc-postgresql/blob/master/src/main/java/io/r2dbc/postgresql/PostgresqlConnectionFactoryProvider.java
     *  https://github.com/r2dbc/r2dbc-postgresql/blob/master/src/main/resources/META-INF/services/io.r2dbc.spi.ConnectionFactoryProvider
     *
     *  More in-depth explanation: https://docs.oracle.com/javase/tutorial/ext/basics/spi.html
     */
    public static ConnectionFactory discover() {
        return ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "postgresql")
                .option(HOST, "127.0.0.1")
                .option(PORT, 5432)  // optional, defaults to 5432
                .option(USER, "postgres")
                .option(PASSWORD, "postgres")
                .option(DATABASE, "postgres")  // optional
                .build());
    }

    /**
     * Programatic driver creation, would not compile if this class is not present
     */
    public static ConnectionFactory postgresConnectionFactory() {
        return new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host("127.0.0.1")
                .port(5432) // optional, defaults to 5432
                .username("postgres")
                .password("postgres")
                .database("postgres")
                .build());
    }

    public static ConnectionFactory H2ConnectionFactory() {
        return new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                .inMemory("goaltracker-database")
                .build());
    }

    /**
     * Wrap an existing connectionfactory with a ProxyConnectionFactory
     * @param connectionFactory original connectionfactory
     * @return ProxyConnectionFactory
     */
    public static ConnectionFactory proxyConnectionFactory(ConnectionFactory connectionFactory) {
        QueryExecutionInfoFormatter queryExecutionFormatter = QueryExecutionInfoFormatter.showAll();

        return ProxyConnectionFactory
                .builder(connectionFactory)
                // on every query execution
                .onAfterQuery(execInfo ->
                        execInfo.map(queryExecutionFormatter::format)    // convert QueryExecutionInfo to String
                                .doOnNext(System.out::println)       // print out executed query
                                .subscribe())
                .build();
    }

    public static ConnectionFactory pooledConnectionFactory() {
        return ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "pool")
                .option(PROTOCOL, "postgresql") // driver identifier, PROTOCOL is delegated as DRIVER by the pool.
                .option(HOST, "127.0.0.1")
                .option(PORT, 5432)  // optional, defaults to 5432
                .option(USER, "postgres")
                .option(PASSWORD, "postgres")
                .option(DATABASE, "postgres")  // optional
                .build());
    }

}
