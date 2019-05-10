# R2dbc playground

This repository contains examples of how to use the R2DBC postgres driver.
It was created for myself to learn Reactor and the R2DBC SPI.
There is also a setup included for a docker postgres instance that logs which statements, 
connections and disconnections on the database server.
This is a useful way to make sure your database operations do what you think they do.

## Reading order

If you are looking to learn about r2dbc-spi usage, start reading from the 
`r2dbc.spi` package in this project. `BasicQuery` and `ConnectionUtil` have the most 
basic query examples. After this the `Transaction` class shows how transaction management 
works. Finally, `r2dbc.spi.QueryUtil` and the `r2dbc.spi.repository` package might 
be of interest to see how a few simple utility functions can lead to pretty concise client code.

## Setup

Use the Dockerfile in the docker_postgres folder to create a postgres container that logs db actions.
Run the following commands from this folder to build and run the postgres container.
The last command will show you all connections and queries being performed on the database.

```
docker build -t postgres_logger .
docker run -d -p 127.0.0.1:5432:5432 --name somename postgres_logger
docker logs --follow somename
```

If you're running a local postgres, take a look at the Dockerfile to see which config lines to change.

The following SQL will create a goal table in the default schema ('postgress')

```sql
create table goal
(
	id serial not null
		constraint goal_pkey
			primary key,
	name varchar(50)
);
```

When this is done, you should be able to go to the `spi.BasicQuery` class, run it, and see multiple blocks like this in 
you terminal.

```
2019-05-04 09:36:33.227 UTC [66] LOG:  connection received: host=172.17.0.1 port=46776
2019-05-04 09:36:33.331 UTC [66] LOG:  connection authorized: user=postgres database=postgres
2019-05-04 09:36:33.371 UTC [66] LOG:  statement: DELETE FROM goal
2019-05-04 09:36:33.401 UTC [66] LOG:  disconnection: session time: 0:00:00.174 user=postgres database=postgres host=172.17.0.1 port=46776
```