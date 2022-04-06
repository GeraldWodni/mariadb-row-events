const MysqlType = require('mysql/lib/protocol/constants/types');
const MysqlTypeArray = Object.keys(MysqlType);

// const fs = require("fs");

const BinlogEvents = {
    0x02: 'QUERY_EVENT',
    0x04: 'ROTATE_EVENT',
    0x0e: 'USERVAR_EVENT',
    0x0f: 'FORMAT_DESCRIPTION_EVENT',
    0x10: 'XID_EVENT',
    0x13: 'TABLE_MAP_EVENT',
    0x17: 'WRITE_ROWS_EVENT',
    0x18: 'UPDATE_ROWS_EVENT',
    0x19: 'DELETE_ROWS_EVENT',
}

const tableMaps = {};

/* https://mariadb.com/kb/en/com_register_slave/ */
class BinlogPacket {
    constructor( opts = {} ) {
        Object.assign( this, {
            timestamp   : undefined,
            eventType   : undefined,
            serverId    : undefined,
            eventLength : undefined,
            logPos      : undefined,
            flags       : undefined,
        }, opts );

        this.skipped = undefined;
        this.data = {};
    }
    

    parse( parser ) {
        const opts = parser._options;

        /* Header */
        try
        {
            this.parseHeader( parser, opts );

            if( this.logPos )
            {
                //opts.last.pos = this.logPos;
                //opts.last.time = this.timestamp;
            }
        }
        catch (err)
        {
            console.log( "Binlog parseHeader", err );
            this.error = err;
            return;
        }

        /* body */
        if( !this.skipped )
            try {
                this.parseBody( parser, opts );
            }
            catch( err ) {
                console.log( "Binlog parseBody", err );
                this.error = this.dataError = err;
            }

        console.log( "BINLOG:", this.toString() );
    }

    parseHeader( parser, opts ) {
        /* https://mariadb.com/kb/en/2-binlog-event-header/ */
        parser.parseUnsignedNumber(1); // marker

        this.timestamp  = parser.parseUnsignedNumber(4);
        this.eventType  = parser.parseUnsignedNumber(1);
        this.serverId   = parser.parseUnsignedNumber(4);
        this.eventLength= parser.parseUnsignedNumber(4);

        this.logPos     = parser.parseUnsignedNumber(4);
        this.flags      = parser.parseUnsignedNumber(2);

        // skip
        //this.skipped = ( opts.last.pos  && opts.last.pos  > this.logPos    )
        //            || ( opts.last.time && opts.last.time > this.timestamp )
    }

    parseBody( parser, opts ) {
        const parseFunctionName = `parse_${this.eventName}`;
        if( parseFunctionName in this )
            this[parseFunctionName].call( this, parser, opts );
        else
            console.log( `HINT: no parser implemeneted for ${this.eventName} (0x${this.eventType.toString(16)})` );

        //switch( this.eventName ) {
        //    case 'ROTATE_EVENT':
        //        const pos1 = parser.parseUnsignedNumber(4);
        //        const pos2 = parser.parseUnsignedNumber(4);
        //        this.data.position = pos1 << 32 | pos2;
        //        this.data.nextBinlogName = parser.parsePacketTerminatedString();
        //        break;
        //    default:
        //        break;
        //}
    }

    /* parsers */
    parse_QUERY_EVENT( parser, opts ) {
        this.data.threadId      = parser.parseUnsignedNumber(4);
        this.data.executionTime = parser.parseUnsignedNumber(4);
        this.data.defaultSchemaLength = parser.parseUnsignedNumber(1);
        this.data.errorCode     = parser.parseUnsignedNumber(2);
        this.data.variableLength= parser.parseUnsignedNumber(2);
    }

    parse_ROTATE_EVENT( parser, opts ) {
        this.data.position = this.parseUnsignedNumber8( parser );
        this.data.nextBinlogName = parser.parsePacketTerminatedString();
    }

    parse_TABLE_MAP_EVENT( parser, opts ) {
        /* Todo: get column names via DESC command? */
        this.data.tableId = this.parseUnsignedNumber6( parser );
        this.data.futureUse = parser.parseUnsignedNumber(2);
        this.data.database = this.parseCountedStringNull( parser, 1 );
        this.data.table = this.parseCountedStringNull( parser, 1 );

        const columnCount = parser.parseLengthCodedNumber();
        this.data.columnTypes = [];
        this.data.columnTypeNames = [];
        for( let i = 0; i < columnCount; i++ ) {
            const columnType = parser.parseUnsignedNumber(1);
            this.data.columnTypes.push( columnType );
            this.data.columnTypeNames.push( MysqlType[ columnType ] );
        }

        const metadataLength = parser.parseLengthCodedNumber();
        this.data.metadataLength = metadataLength;
        this.data.metadata = parser.parseBuffer(this.data.metadataLength);
        this.data.nullColumns = this.parseBitfield( parser, columnCount );

        // metadata encodes the dynamic length for the following types
        // https://mariadb.com/kb/en/rows_event_v1v2/#column-data-formats
        const twoBytesLength = [ 'BIT', 'ENUM', 'SET', 'NEWDECIMAL', 'DECIMAL', 'VARCHAR', 'VAR_STRING', 'STRING' ];
        const oneByteLength  = [ 'TINY_BLOB', 'MEDIUM_BLOB', 'LONG_BLOB', 'BLOB', 'FLOAT', 'DOUBLE', 'TIMESTAMP2', 'DATETIME2', 'TIME2 ' ];

        this.data.columnLengths = [];
        let offset = 0;
        this.data.columnTypeNames.forEach( columnTypeName => {
            var length = null;
            if( oneByteLength.indexOf( columnTypeName ) >= 0 )
                length = this.data.metadata.readUint8( offset++ );
            else if( twoBytesLength.indexOf( columnTypeName ) >= 0 ) {
                length = this.data.metadata.readUint16LE( offset++ );
                offset++;
            }
            this.data.columnLengths.push( length );
        });

        tableMaps[ this.data.tableId ] = {
            database: this.data.database,
            table: this.data.table,
            columnTypes: this.data.columnTypes,
            columnTypeNames: this.data.columnTypeNames,
            nullColumns: this.data.nullColumns,
            columnLengths: this.data.columnLengths,
        }
    }

    parse_WRITE_ROWS_EVENT( parser, opts ) {
        this.data.tableId = this.parseUnsignedNumber6( parser );
        this.data.flags = parser.parseUnsignedNumber(2);
        this.data.columnCount = parser.parseLengthCodedNumber();
        this.data.usedColumns = this.parseBitfield( parser, this.data.columnCount );
        this.data.nullColumns = this.parseBitfield( parser, this.data.columnCount );
        this.data.tableMap = tableMaps[ this.data.tableId ];

        this.data.columns = this.parseColumnData( parser, opts );
    }

    parse_XID_EVENT( parser, opts ) {
        this.data.xid = this.parseUnsignedNumber8( parser );
    }

    /* parser helpers */
    parseColumnData( parser, opts ) {
        //fs.writeFileSync( `captured/write-row-${this.logPos}`, parser.parsePacketTerminatedBuffer() );
        //return;

        const columns = [];
        for( var i = 0; i < this.data.columnCount; i++ ) {
            if( this.data.nullColumns[i] ) {
                columns.push( null );
                continue;
            }
            /* See https://mariadb.com/kb/en/rows_event_v1v2/#column-data-formats */
            switch( this.data.tableMap.columnTypeNames[i] ) {
                /* simple types */
                case 'TINY':
                    columns.push( parser.parseUnsignedNumber(1) );
                    break;
                case 'SHORT':
                case 'YEAR':
                    columns.push( parser.parseUnsignedNumber(2) );
                    break;
                case 'INT24':
                    columns.push( parser.parseUnsignedNumber(3) );
                    break;
                case 'LONG':
                    columns.push( parser.parseUnsignedNumber(4) );
                    break;
                case 'LONGLONG':
                    columns.push( this.parseUnsignedNumber8( parser ) );
                    break;

                case 'FLOAT':
                    columns.push( this.parseFloat( parser ) );
                    break;
                case 'DOUBLE':
                    columns.push( this.parseDouble( parser ) );
                    break;

                case 'DATETIME2': {
                        columns.push( { dl: this.data.tableMap.columnLengths[i], data: parser.parseBuffer(5) } );
                        //columns.push( new Date( parser.parseLengthCodedString() ) );
                    }
                    break;

                case 'VARCHAR': {
                        let length = 0;
                        if( this.data.tableMap.columnLengths[i] > 255 )
                            length = parser.parseUnsignedNumber(2);
                        else
                            length = parser.parseUnsignedNumber(1);

                        columns.push( parser.parseString(length) );
                    }
                    break;

                case 'STRING': {
                        const metaData = this.data.tableMap.columnLengths[i];
                        const length = metaData >> 8;
                        columns.push({
                            type:   MysqlType[ metaData & 0xFF ],
                            length,
                            data:   parser.parseBuffer( length ),
                        });
                    }
                    break;

                case 'BLOB': {
                        const intLength = this.data.tableMap.columnLengths[i];
                        const length = parser.parseUnsignedNumber( intLength );
                        columns.push( parser.parseString( length ) );
                    }
                    break;

                default:
                    columns.push( { "undefined": true } );
            }
        }
        console.log( this.logPos, JSON.stringify( columns ) );
        return columns;
    }

    parseBitfield( parser, count ) {
        const length = Math.floor((count + 7)/8);
        const bitfield = parser.parseBuffer( length );

        const values = [];
        for( var i = 0; i < count; i++ )
            values[i] = bitfield[ Math.floor(i/8) ] >> (i%8) & 0x01;

        return values;
    }

    parseFloat( parser ) {
        const buffer = parser.parseBuffer(4);
        return buffer.readFloatLE();
    }
    parseDouble( parser ) {
        const buffer = parser.parseBuffer(8);
        return buffer.readDoubleLE();
    }
    parseCountedStringNull( parser, intLength ) {
        const length = parser.parseUnsignedNumber( intLength );
        return this.parseStringNull( parser, length );
    }
    parseStringNull( parser, length ) {
        const name = parser.parseString( length );
        parser.parseUnsignedNumber(1); // read terminating Null
        return name;
    }
    parseUnsignedNumber8( parser ) {
        const pos1 = parser.parseUnsignedNumber(4);
        const pos2 = parser.parseUnsignedNumber(4);
        return pos1 << 32 | pos2;
    }
    parseUnsignedNumber6( parser ) {
        const pos1 = parser.parseUnsignedNumber(2);
        const pos2 = parser.parseUnsignedNumber(4);
        return pos1 << 32 | pos2;
    }

    /* reflection */
    get eventName() {
        if( this.eventType in BinlogEvents )
            return BinlogEvents[ this.eventType ];

        return 'UNKNOWN_EVENT';
    }

    toString() {
        return this.eventName + ': ' + JSON.stringify( {
            //timestamp: this.timestamp,
            //eventType: this.eventType,
            //serverId:  this.serverId,
            //eventLength: this.eventLength,
            //logPos:    this.logPos,
            //flags:     this.flags,
            data:      this.data,
        } );//, null, 4 );
    }

};

module.exports = BinlogPacket;

