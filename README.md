### mysql-connector

Simple example of mysql connector for jetstream (nats.io) .

#### Prerequisite
1. Jetstream enabled nats server [LINK](https://github.com/nats-io/jetstream#getting-started)

2. Jetstream enabled golang client library [LINK](https://github.com/nats-io/nats.go). Will probably need pre-released version

#### How to run
```go main.go -startfrom=1```

#### Config option
 ##### 1. dsn
 Database conenction string
 ##### 2. table
 Table to source data from
 ##### 3. idColumn
 Autoincrement column name
 ##### 4. js
 NATS connection string
 ##### 5. subject
 NATS subject to publish the message to
 
### Restarting
In case the connector is restarted and need to continue from where it left, first figure out the last message in the stream. Use [natscli](https://github.com/nats-io/natscli)

- Get the last sequence of the stream with :
```nats str info```
- Fetch the last message with
```nats req '$JS.API.STREAM.MSG.GET.ACCOUNT_TRANSACTION' '{"seq":<LastSequence>}'```
 - Base64 decode the `data` field and obtain the autoincrement value. Use that value + 1for `-startfrom` param
