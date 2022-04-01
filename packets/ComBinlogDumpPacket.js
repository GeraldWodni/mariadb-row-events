/* https://mariadb.com/kb/en/com_binlog_dump/ */
class ComBinlogDumpPacket {
    constructor( opts = {} ) {
        Object.assign( this, {
            command         : 0x12,
            position        : 4,
            flags           : 0,
            serverId        : 1,
            binlogFilename  : ''
        }, opts );
    }

    write( writer ) {
        console.log( "write ComBinlogDumpPacket", this.position );
        writer.writeUnsignedNumber(1, this.command);
        writer.writeUnsignedNumber(4, this.position);
        writer.writeUnsignedNumber(2, this.flags);
        writer.writeUnsignedNumber(4, this.serverId);
        writer.writeNullTerminatedString( this.binlogFilename );
    };
}

module.exports = ComBinlogDumpPacket;

