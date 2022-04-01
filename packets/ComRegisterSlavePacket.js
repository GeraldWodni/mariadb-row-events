/* https://mariadb.com/kb/en/com_register_slave/ */
class ComRegisterSlavePacket {
    constructor( opts = {} ) {
        Object.assign( this, {
            command         : 0x15,
            serverId        : 1,
            hostname        : '',
            user            : '',
            password        : '',
            port            : null,
            replicationRank : 0,
            masterId        : 0,
        }, opts );
    }

    write( writer ) {
        writer.writeUnsignedNumber(1, this.command);
        writer.writeUnsignedNumber(4, this.serverId);
        writer.writeUnsignedNumber(1, this.hostname.length);
        writer.writeNullTerminatedString(this.hostname);
        writer.writeUnsignedNumber(1, this.user.length);
        writer.writeNullTerminatedString(this.user);
        writer.writeUnsignedNumber(1, this.password.length);
        writer.writeNullTerminatedString(this.password);
        writer.writeUnsignedNumber(2, this.port);
        writer.writeUnsignedNumber(4, this.replication_rank);
        writer.writeUnsignedNumber(4, this.master_id);
    };
}

module.exports = ComRegisterSlavePacket;

