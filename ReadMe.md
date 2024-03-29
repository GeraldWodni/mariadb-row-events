# mariadb-row-events
## Replication slave that emits rows as events 

This project is loosly based on [mysql-binlog-emitter](https://github.com/p80-ch/mysql-binlog-emitter), which is a wonderful library but lacks the support for row-values.
As the replication protocol documentation is terse, mysql-binlog-emitter gave valuable insight to how the replication protocol works, so big thanks to the folks over there.

__Important:__ This library only supports mariadb, as mysql uses additional packets which have not been implemented here. If you need mysql support I can help you get started (open an issue), or you can insert coins.

## Usage
```javascript
const config = {
    mysql: {
        host:     "localhost",
        port:     "3306",
        database: "exampledb",
        user:     "exampleuser",
        password: "examplepassword",
    },
    binlog: {
        position: 4,   // starting position for each new log, you should store the last value of that (available as packet.logPos),
        binlogFilename: "replication_bin.000007", // watch for rotate events and use last value of nextBinlogName
    },
    logPackets: false, // will log every packet, useful for debugging
    skipUntilLogPos: 4, // if binlog.position fails with "Client requested master to start replication from impossible position", use this to skip client side
    skipUntilTimestamp: 0, // skips until timestamp greater, then skips until logPos is greater-equals
}

// create new instance
const mariadbRowEvents = new MariadbRowEvents( config );
// hookup error events
mariadbRowEvents.on('fatal', err => {
    console.log( "Fatal", err );
    process.exit(1);
});
mariadbRowEvents.on('mysql-error', err => {
    console.log( "Mysql", err );
    process.exit(2);
});
// hookup custom events
mariadbRowEvents.on("customers-insert", event => event.rows.forEach( row => {
    console.log( "New customer:", row.columns );
}) );
// connect
mariadbRowEvents.connect();
```

## Installation
`npm install mariadb-row-emitter`

## Events
### `skipped`
The event has not been handled by any other

### `insert`
New row INSERTed  
Hint: this also emits an event `<table-name>` as well as `<table-name>-insert`.

__Example:__
```javascript
{
  logPos: 12164764,
  operation: "insert",
  database: "exampledb",
  table: "exampletable",
  rows: [{
      columnsArray: [        // raw columns (as parsed from the packet)
        1234,
        "exampleuser",
        { type: "ENUM", length: 1, data: <Buffer 01> },
        2022-07-01T00:00:00.000Z,
        { intLength: 2, length: 6, blob: "Fly you fools" },
        null,
      ],
      keys: {
        primaryColumns: [ "id" ],
        primaryValues: [ 1234 ],
        primaryValue: "1234" // if there are multiple primaryValues, they are joined by `-`.
      },
      columns: {             // object with decoded ENUMs, TEXTs and BLOBs ...
        id: 9266,            // `columns` is `null` if table definition differs from this record
        name: "exampleuser", // i.e. because of an `ALTER TABLE` statement in between
        type: "administrator",
        registered: 2022-07-01T00:00:00.000Z,
        motto: "Fly you fools",
        avatarImage: null,
      }
  }] // Note: there can several rows per event, but they share the same logPos, so they are kept together
}
```

### `update`
Existing Row UPDATEd  
Hint: this also emits an event `<table-name>` as well as `<table-name>-update`.

__Example:__
```javascript
{
  logPos: 12164764,
  operation: "insert",
  database: "exampledb",
  table: "exampletable",
  rows: [{
      oldColumnsArray: [
        1234,
        "exampleuser",
        { type: "ENUM", length: 1, data: <Buffer 01> },
        2022-07-01T00:00:00.000Z,
        { intLength: 2, length: 6, blob: "Fly you fools" },
        null,
      ],
      columnsArray: [
        1234,
        "exampleuser",
        { type: "ENUM", length: 1, data: <Buffer 01> },
        2022-07-01T00:00:00.000Z,
        { intLength: 2, length: 6, blob: "Fool of a Took" },
        null,
      ],
      oldKeys: {
        primaryColumns: [ "id" ],
        primaryValues: [ 1234 ],
        primaryValue: "1234"
      },
      keys: {
        primaryColumns: [ "id" ],
        primaryValues: [ 1234 ],
        primaryValue: "1234"
      },
      oldColumns: {
        id: 9266,
        name: "exampleuser",
        type: "administrator",
        registered: 2022-07-01T00:00:00.000Z,
        motto: "Fly you fools",
        avatarImage: null,
      }
      columns: {
        id: 9266,
        name: "exampleuser",
        type: "administrator",
        registered: 2022-07-01T00:00:00.000Z,
        motto: "Fool of a Took",
        avatarImage: null,
      },
      changedColumns: {
        motto: {
            old: "Fly you fools",
            new: "Fool of a Took",
        }
      }
  }]
}
```

### `delete`
Existing Row DELETEd  
Hint: this also emits an event `<table-name>` as well as `<table-name>-delete`.

### `fatal`
Something went very wrong, you should exit.

__Example:__
```javascript
    mariadbRowEvents.on('fatal', err => {
        console.log( "Fatal", err );
        process.exit(1);
    });
```

### `mysql-error`
MySQL (mariadb) threw an error, you should also exit in that case.

__Example:__
```javascript
mariadbRowEvents.on('mysql-error', err => {
    console.log( "Mysql", err );
    process.exit(2);
});
```

### `rotate`
New binlog file is used. Either due to size-limit or server-restart.
Provide this value as connect-opts to ensure no useless transmission and double-firing

## slaveId
Different repliaction instances need different IDs. I recommand using an environment variable and set them like so:
```javascript
if( process.env.MYSQL_SLAVE_SERVER_ID ) {
    config.binlog.serverId = process.env.MYSQL_SLAVE_SERVER_ID;
    config.slave = { serverId: process.env.MYSQL_SLAVE_SERVER_ID };
}
```

## Security
I would like to point out that storing credentials in source code is at best okay for testing. Use environment variables instead i.e. like so:
```javascript
if( process.env.MYSQL_HOST      ) config.mysql.host     = process.env.MYSQL_HOST;
if( process.env.MYSQL_PORT      ) config.mysql.port     = process.env.MYSQL_PORT;
if( process.env.MYSQL_DATABASE  ) config.mysql.database = process.env.MYSQL_DATABASE;
if( process.env.MYSQL_USER      ) config.mysql.user     = process.env.MYSQL_USER;
if( process.env.MYSQL_PASSWORD  ) config.mysql.password = process.env.MYSQL_PASSWORD;
```

