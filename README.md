## A Project on Concurrency + Server/Client Communication ##

This is a small project I'm working on to learn concurrency and how servers communicate with clients.

It's a small Redis-compatible server in Java: it speaks the Redis protocol (RESP)
over TCP, serves each connection on its own virtual thread, and keeps all data in
memory.

Supported commands: `PING`, `ECHO`, `SET`, `GET`, `INCR`, `LPUSH`, `RPUSH`,
`LPOP`, `RPOP`, `LLEN`, `BLPOP` (single key).

## Quick start

Requires Java 21+ and Maven.

```bash
mvn verify                              # compile and run all tests
java -cp target/classes server.Server   # start the server on port 6380
```

Then, from another terminal:

```bash
redis-cli -p 6380 ping                  # if you have redis-cli installed
nc localhost 6380                       # or type commands by hand: PING, SET k v, GET k
```

To run a single test class: `mvn test -Dtest=BlpopTest`.

The server listens on 6380 rather than Redis's default 6379, so it won't clash
with a real Redis running on the same machine.
