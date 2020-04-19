# Mini Queue Service (mqs)

mqs is a service able to receive, store and delete messages in one or multiple queues.

Features:

* [X] Delayed messages.
* [X] Making messages invisible for a fixed duration after receiving a message.
* [X] Message deduplication for all messages *currently* in a single queue (if configured).
* [X] Sending messages to a different queue after too many receives.
* [X] Long-polling until a message arrives.
* [ ] Authentication and multi-user management.
* [ ] Dashboard to view current server status and/or manage server.

## Setting up a server

To get a quick basic setup running, we need to start a postgres server and then can launch our mqs server.
Keep in mind that neither the username/password of the database nor mqs are secured in any way by this setup,
so don't use this for anything besides a quick test!

Launch a postgres server:

```shell script
$ docker run \
    --name mqs-postgres \
    --publish 5432:5432 \ 
    --env POSTGRES_USER=user \
    --env POSTGRES_PASSWORD=password \
    --env POSTGRES_DB=mqs \
    postgres:11.2
```

We now have a temporary database server running to which we can connect if needed.
Keep in mind that all the data on this server will be deleted after the container is deleted.

Launch mqs:

```shell script
$ docker run --detach \
    --name mqs \
    --publish 7843:7843 \
    --env DATABASE_URL=postgres://user:password@localhost/mqs \
    --env MIN_POOL_SIZE=5 \
    --env MAX_POOL_SIZE=25 \
    --env MAX_MESSAGE_SIZE=1048576 \
    ajscholl/mqs:latest
```

This will start a new instance listening on port 7843 (default port, you currently can't change this) accepting requests
up to 1MiB in size. Between 5 and 25 connections to the database will be kept open at all times.
It will also already setup our database schema - mqs runs database migrations automatically on startup by default.

**Keep in mind that there is no authentication at all in the current version of mqs, so you maybe don't want to expose
the post mqs listens on to the internet!**

## Talking to mqs via HTTP

Documentation about the different routes you can call can be found on [Swagger](https://app.swaggerhub.com/apis/ajscholl/mqs/1.0.0).
