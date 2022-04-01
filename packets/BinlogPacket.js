const MysqlType = require('mysql/lib/protocol/constants/types');
const MysqlTypeArray = Object.keys(MysqlType);

const BinlogEvents = {
    0x04: 'ROTATE_EVENT',
}

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
        console.log( "Parse header started" );
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
        console.log( "Parse header ended" );
    }

    parseBody( parser, opts ) {
        switch( this.eventName ) {
            case 'ROTATE_EVENT':
                console.log( "ROTATE IN DA HOUSE", this.data );
                const pos1 = parser.parseUnsignedNumber(4);
                const pos2 = parser.parseUnsignedNumber(4);
                this.data.position = pos1 << 32 | pos2;
                this.data.nextBinlogName = parser.parsePacketTerminatedString();
                break;
            default:
                break;
        }
    }

    get eventName() {
        if( this.eventType in BinlogEvents )
            return BinlogEvents[ this.eventType ];

        return 'UNKNOWN';
    }

    toString() {
        return this.eventName + ': ' + JSON.stringify( {
            timestamp: this.timestamp,
            eventType: this.eventType,
            serverId:  this.serverId,
            eventLength: this.eventLength,
            logPos:    this.logPos,
            flags:     this.flags,
            data:      this.data,
        }, null, 4 );
    }

};

module.exports = BinlogPacket;

