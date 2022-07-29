/* mariadb replication slave that emits rows as events
 * (c)copyright 2022 by Gerald Wodni <gerald.wodni@gmail.com>
 *
 * This project is loosly based on
 * https://github.com/p80-ch/mysql-binlog-emitter
 * which is a wonderful library but lacks the support for row-values
 */

const util = require("util");

const EventEmitter = require('events');
const mysql = require('mysql');

const RegisterSlave = require("./sequences/RegisterSlave");
const Binlog = require("./sequences/Binlog");

class MariadbRowEvents extends EventEmitter {
    constructor( opts ) {
        super();

        this.opts = opts;

        /* skipUntil "<timestamp>-<logPos>" */
        if( typeof this.opts.skipUntil == "undefined" )
            this.opts.skipUntil = "0-0";
        const parts = this.opts.skipUntil.split("-");
        if( parts.length == 2 ) {
            this.skipUntilTimestamp = Number(parts[0]);
            this.skipUntilLogPos = Number(parts[1]);
        }
        else {
            this.skipUntilTimestamp = 0;
            this.skipUntilLogPos = Number(parts[0]);
        }

        this.pool = mysql.createPool( opts.mysql );
    }

    async connect() {
        try {
            /* check compatibility */
            const data = await this.query( "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME LIKE 'MASTER_VERIFY_CHECKSUM'")
            if( data.length > 0 && data[0].VARIABLE_VALUE == "ON" )
                return this.emitError( 'fatal', new Error( "Checksums enabled, set binlog_checksum=NONE in [mariadb] config" ) );

            /* fetch table information */
            await this.getTables();
        }
        catch( err ) {
            return this.emitError( 'fatal', err );
        }

        /* replication connection */
        this.pool.getConnection( (err, connection) => {
            if( err )
                return this.emitError( 'error', err );

            this.connection = connection;
            this.bindErrors( this.connection );
            this.connection._implyConnect();

            const registerSlave = new RegisterSlave( this.opts.slave );
            this.bindErrors( registerSlave );
            registerSlave.on( 'end', () => {
                console.log( "registered!" );

                this.binlog = new Binlog( { binlog: this.opts.binlog }, null, this.binlogPacket.bind( this ) );
                this.bindErrors( this.binlog );
                this.binlog.on( 'end', () => console.log( "(BINLOG END)" ) );

                this.connection._protocol._enqueue( this.binlog );
            });

            this.connection._protocol._enqueue( registerSlave );
        });
    }

    async getTables() {
        const tables = ( await this.query( "SHOW TABLES" ) ).map( row => row[ Object.keys(row)[0] ] );
        this.tables = {};

        const promises = [];
        for( const table of tables )
            promises.push( this.query( "DESC ??", [table] ) );
        let i = 0;
        for( const tableInfo of await Promise.all( promises ) ) {
            const tableName = tables[i++];
            const columns = [];
            for( const columnInfo of tableInfo ) {
                const column = {
                    name:   columnInfo.Field,
                    sqlType:columnInfo.Type,
                    type:   columnInfo.Type,
                    null:   columnInfo.Null == "YES",
                    default:columnInfo.Default,
                }

                /* optional data */
                if( columnInfo.Key )
                    column.key = columnInfo.Key;
                if( columnInfo.Extra )
                    column.extra = columnInfo.Extra;

                /* additional type information */
                if( column.type.indexOf( "(" ) > 0 )
                    column.type = column.type.split("(")[0];
                if( column.type == "enum" )
                    column.enum = column.sqlType.replace("enum(", "").replace(")", "").replace(/'/g, "").split(",");

                columns.push( column );
            }
            this.tables[ `${this.opts.mysql.database}.${tableName}` ] = columns;
        }
    }

    binlogPacket( err, packet ) {
        //console.log( "Got binlogPacket:", err, packet );

        if( err )
            return this.emitError( 'binlog-error', err );

        if( packet.error || packet.data_error ) {
            const error = packet.error;
            const dataError = packet.dataError;

            delete packet.error;
            delete packet.dataError;

            return this.emitError( 'data-error', dataError || error );
        }

        if( this.skipUntilTimestamp > packet.timestamp )
            return console.log( `TIMESTAMP SKIPPING UNTIL ${this.skipUntilTimestamp} > ${packet.timestamp}` );
        if( this.skipUntilLogPos >= packet.logPos )
            return console.log( `LOGPOS SKIPPING UNTIL ${this.skipUntilLogPos} >= ${packet.logPos}` );

        if( this.opts.logPackets ) {
            const { ignore, text } = packet.getShortString();
            if( !ignore || this.opts.logPackets == "all" )
                console.log( text );
        }

        switch( packet.eventName ) {
            case 'DELETE_ROWS_EVENT': 
            case 'UPDATE_ROWS_EVENT': 
            case 'WRITE_ROWS_EVENT': 

                const operation = packet.eventName.replace("_ROWS_EVENT", "").toLowerCase().replace("write", "insert");
                const rowsEvent = {
                    logPos:   packet.logPos,
                    timestamp:packet.timestamp,
                    operation,
                    database: packet.data.tableMap.database,
                    table:    packet.data.tableMap.table,
                    rows: packet.data.rows,
                };

                for( const row of rowsEvent.rows ) {
                    const { keys, columns } = this.arrayToColumns( rowsEvent.database, rowsEvent.table, row.columnsArray );
                    row.keys = keys;
                    row.columns = columns;
                    if( packet.eventName == 'UPDATE_ROWS_EVENT' ) {
                        const { keys: oldKeys, columns: oldColumns } = this.arrayToColumns( rowsEvent.database, rowsEvent.table, row.oldColumnsArray );
                        row.oldKeys = oldKeys;
                        row.oldColumns = oldColumns;
                        row.changedColumns = this.changedColumns( row.oldColumns, row.columns );
                    }
                }

                //console.log( operation, rowsEvent.table, rowsEvent.keys?.primaryValue );
                this.emit( operation, rowsEvent );
                this.emit( rowsEvent.table, rowsEvent );
                this.emit( rowsEvent.table + '-' + operation, rowsEvent );
                return;
        }

        this.emit( 'skipped', packet );
    }

    changedColumns( oldColumns, newColumns ) {
        const changes = {};
        if( oldColumns == null || newColumns == null )
            return changes;

        for( const key of Object.keys( newColumns ) )
            if( oldColumns[ key ] != newColumns[ key ] )
                changes[ key ] = { old: oldColumns[key], new: newColumns[key] };

        return changes;
    }

    arrayToColumns( database, tableName, columnsArray ) {
        const tableColumns = this.tables[ `${database}.${tableName}` ];
        if( typeof tableColumns == "unknown" ) {
            console.log( `WARNING: ColumnArray unknown table: ${database}.${tableName}` );
            return { keys: null, columns: null };
        }

        if( columnsArray.length != tableColumns.length ) {
            console.log( `WARNING: ColumnArray length missmatch in ${database}.${tableName}: ${tableColumns.length} columns in DESC, but ${columnsArray.length} in EVENT` );
            return { keys: null, columns: null };
        }

        const columns = {};
        const keys = {
            primaryColumns: [],
            primaryValues: [],
            primaryValue: null,
        };
        for( let i = 0; i < tableColumns.length; i++ ) {
            const tableColumn = tableColumns[i];
            let value = columnsArray[i];
            switch( tableColumn.type.toUpperCase() ) {
                case 'ENUM':
                    try {
                        value = tableColumn.enum[ value.data[0] - 1 ];
                    } 
                    catch( err ) {
                        console.log( "WARNING: ColumnArray enum Error:", err )
                        value = null;
                    }
                    break;
                case 'TINYTEXT':
                case 'MEDIUMTEXT':
                case 'LONGTEXT':
                case 'TEXT':
                case 'TINYBLOB':
                case 'MEDIUMBLOB':
                case 'LONGBLOB':
                case 'BLOB':
                    if( value != null ) {
                        value = value.blob;
                    }
                    break;
            }
            columns[ tableColumn.name ] = value;

            if( tableColumn.key == "PRI" ) {
                keys.primaryColumns.push( tableColumn.name );
                keys.primaryValues.push( value );
            }
        }

        if( keys.primaryValues.length )
            keys.primaryValue = keys.primaryValues.map( v => v.toString() ).join("-");

        return { keys, columns };
    }

    bindErrors( sequence, errorType = 'mysql-error', events = [ 'error', 'unhandledError', 'timeout' ] ) {
        events.forEach( event => sequence.on( event, this.emitError.bind( this, errorType ) ) );
    }

    emitError( type, err ) {
        this.emit( type, err );

        /* also emit generic error event */
        if( err.type != 'error' ) {
            err.type = type;
            this.emit( 'error', err );
        }
    }

    query( sql, parameters = [] ) {
        return new Promise( (fulfill, reject) => 
            this.pool.query( sql, parameters, ( err, data ) => {
                if( err )
                    return reject( err );
                fulfill( data );
            })
        );
    }
}

/* https://dev.mysql.com/doc/internals/en/rows-event.html */

module.exports = MariadbRowEvents;

/* quick debug function which logs all packets to stdout */
async function main() {
    const config = {
        mysql: {
            host: "localhost",
            port: "3306",
        },
        binlog: {
            //position: 68397,
            position: 4,
        },
        logPackets: false,
        skipUntil: "1657792732-0",
    }
    if( process.env.MYSQL_HOST      ) config.mysql.host     = process.env.MYSQL_HOST;
    if( process.env.MYSQL_PORT      ) config.mysql.port     = process.env.MYSQL_PORT;
    if( process.env.MYSQL_DATABASE  ) config.mysql.database = process.env.MYSQL_DATABASE;
    if( process.env.MYSQL_USER      ) config.mysql.user     = process.env.MYSQL_USER;
    if( process.env.MYSQL_PASSWORD  ) config.mysql.password = process.env.MYSQL_PASSWORD;
    if( process.env.MYSQL_SLAVE_SERVER_ID ) {
        config.binlog.serverId = process.env.MYSQL_SLAVE_SERVER_ID;
        config.slave = { serverId: process.env.MYSQL_SLAVE_SERVER_ID };
    }
    console.log("config:", config);

    const mariadbRowEvents = new MariadbRowEvents( config );
    mariadbRowEvents.on('fatal', err => {
        console.log( "Fatal", err );
        process.exit(1);
    });
    mariadbRowEvents.on('mysql-error', err => {
        console.log( "Mysql", err );
        process.exit(2);
    });
    mariadbRowEvents.on("customers-insert", evt => evt.rows.forEach( row => {
        console.log( "New customer:", row.columns );
    }) );
    mariadbRowEvents.on("customers-update", evt => evt.rows.forEach( row => {
        console.log( "Update customer:", row.changedColumns );
        //console.log( JSON.stringify( evt, null, 4 ) );
    }) );
    mariadbRowEvents.connect();
    //console.log( mariadbRowEvents.tables );
}

if( require.main == module )
    main();
