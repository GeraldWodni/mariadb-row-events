const Packets = require('mysql/lib/protocol/packets');
const Sequence = require('mysql/lib/protocol/sequences/Sequence');

const ComRegisterSlavePacket = require('../packets/ComRegisterSlavePacket');

class RegisterSlave extends Sequence {
    constructor( opts ) {
        super( opts );

        this.opts = opts;
    }

    start() {
        this.emit( 'packet', new ComRegisterSlavePacket( this.opts ) );
    }
}

module.exports = RegisterSlave;

