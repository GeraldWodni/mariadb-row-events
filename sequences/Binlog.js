const Packets = require('mysql/lib/protocol/packets');
const Sequence = require('mysql/lib/protocol/sequences/Sequence');

const BinlogPacket = require('../packets/BinlogPacket');
const ComBinlogDumpPacket = require('../packets/ComBinlogDumpPacket');

class Binlog extends Sequence {
    constructor( opts, callbackEnd, callbackPacket ) {
        super( opts, callbackEnd );

        this.opts = opts;
        this.callbackPacket = callbackPacket;

        this.handshaked = false;
    }

    start() {
        this.emit( 'packet', new ComBinlogDumpPacket( this.opts.binlog ) );
    }

    determinePacket( firstByte, parser ) {
        switch( firstByte ) {
            case 0x00:
                if( !this.handshaked ) {
                    this.handshaked = true;
                    this.emit('handshake');
                }

                return BinlogPacket;

            case 0xfe:
                return Packets.EofPacket;

            case 0xff:
                return Packets.ErrorPacket;

            default:
                return undefined;
        }
    }

    BinlogPacket( packet ) {
        this.callbackPacket( null, packet );
    }
}

module.exports = Binlog;

