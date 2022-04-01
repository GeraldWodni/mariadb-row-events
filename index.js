/* mariadb replication slave that emits rows as events
 * (c)copyright 2022 by* Gerald Wodni <gerald.wodni@gmail.com>
 *
 * This project is loosly based on
 * https://github.com/p80-ch/mysql-binlog-emitter
 * which is a wonderful library but lacks the support for row-values
 */

const EventEmitter = require('events');
const mysql = require('mysql');

const RegisterSlave = require("./sequences/RegisterSlave");
const Binlog = require("./sequences/Binlog");

class MariadbRowEvents extends EventEmitter {
    constructor( opts ) {
        super();

        this.opts = opts;

        this.pool = mysql.createPool( opts.mysql );
        this.pool.query( "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME LIKE 'MASTER_VERIFY_CHECKSUM'", (err, data) => {
            if( err )
                return this.emitError( 'fatal', err );
            if( data.length > 0 && data[0].VARIABLE_VALUE == "ON" )
                return this.emitError( 'fatal', new Error( "Checksums enabled, set binlog_checksum=NONE in [mariadb] config" ) );
        });
    }

    connect() {
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

                this.binlog = new Binlog( { }, null, this.binlogPacket.bind( this ) );
                this.bindErrors( this.binlog );
                this.binlog.on( 'end', () => console.log( "(BINLOG END)" ) );

                this.connection._protocol._enqueue( this.binlog );
            });

            this.connection._protocol._enqueue( registerSlave );
        });
    }

    binlogPacket( err, packet ) {
        return;
        console.log( "Got binlogPacket:", err, packet );

        if( err )
            return this.emitError( 'binlog-error', err );

        if( packet.error || packet.data_error ) {
            const error = packet.error;
            const dataError = packet.dataError;

            delete packet.error;
            delete packet.dataError;

            return this.emitError( 'data-error', dataError || error );
        }

        this.emit( 'skipped', packet );
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
}

/* https://dev.mysql.com/doc/internals/en/rows-event.html */

module.exports = MariadbRowEvents;

/* quick debug function which logs all packets to stdout */
function main() {
    const config = {
        mysql: {
            host: "localhost",
            port: "3306",
        }
    }
    if( process.env.MYSQL_HOST      ) config.mysql.host     = process.env.MYSQL_HOST;
    if( process.env.MYSQL_PORT      ) config.mysql.port     = process.env.MYSQL_PORT;
    if( process.env.MYSQL_DATABASE  ) config.mysql.database = process.env.MYSQL_DATABASE;
    if( process.env.MYSQL_USER      ) config.mysql.user     = process.env.MYSQL_USER;
    if( process.env.MYSQL_PASSWORD  ) config.mysql.password = process.env.MYSQL_PASSWORD;
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
    mariadbRowEvents.connect();
}

if( require.main == module )
    main();
