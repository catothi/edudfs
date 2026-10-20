package edudfs.protocol;

import edudfs.common.DataChecksum;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Block semantics and wire formats of the data channel. */
class ProtocolTest {

    // ---------------------------------------------------------------- Block
    @Test
    void blockEqualityUsesOnlyTheId() {
        Block a = new Block(1073741825L, 100, 1001);
        Block b = new Block(1073741825L, 999, 1005);  // different length, different GS
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, new Block(1073741826L, 100, 1001));
    }

    @Test
    void blockCanBeLookedUpWithDifferentGenerationStamp() {
        Map<Block, String> map = new HashMap<>();
        map.put(new Block(7, 0, 1001), "stored");
        assertEquals("stored", map.get(new Block(7, 12345, 2000)));
    }

    @Test
    void blockNaming() {
        Block b = new Block(1073741825L, 0, 1001);
        assertEquals("blk_1073741825", b.getBlockName());
        assertEquals("blk_1073741825_1001", b.toString());
    }

    @Test
    void datanodeIdentityIsTheUuid() {
        DatanodeID a = new DatanodeID("uuid-1", "localhost", 50010, 50020);
        DatanodeID moved = new DatanodeID("uuid-1", "otherhost", 1, 2);
        assertEquals(a, moved);
        assertEquals(a, new DatanodeInfo(moved));
        assertNotEquals(a, new DatanodeID("uuid-2", "localhost", 50010, 50020));
        assertEquals("localhost:50010", a.getXferAddr());
    }

    // ----------------------------------------------------------- wire formats
    private static DataInputStream roundTrip(IoWriter w) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        w.write(out);
        out.flush();
        return new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
    }

    interface IoWriter {
        void write(DataOutputStream out) throws IOException;
    }

    @Test
    void packetRoundTrip() throws IOException {
        byte[] data = new byte[1000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        int[] sums = DataChecksum.computeChunkedSums(data, 0, data.length);
        Packet p = new Packet(65536, 7, false, data, data.length, sums);
        Packet q = Packet.read(roundTrip(p::write));
        assertEquals(65536, q.offsetInBlock);
        assertEquals(7, q.seqno);
        assertFalse(q.lastPacketInBlock);
        assertArrayEquals(data, q.data);
        assertArrayEquals(sums, q.checksums);
    }

    @Test
    void lastPacketIsEmpty() throws IOException {
        Packet q = Packet.read(roundTrip(Packet.lastPacket(131072, 3)::write));
        assertTrue(q.lastPacketInBlock);
        assertEquals(0, q.dataLen);
        assertEquals(0, q.checksums.length);
    }

    @Test
    void packetWithWrongChecksumCountIsRejected() {
        Packet bad = new Packet(0, 0, false, new byte[1024], 1024, new int[1]);
        assertThrows(IOException.class, () -> Packet.read(roundTrip(bad::write)));
    }

    @Test
    void pipelineAckRoundTrip() throws IOException {
        short[] replies = {DataTransferProtocol.SUCCESS, DataTransferProtocol.SUCCESS, DataTransferProtocol.ERROR};
        PipelineAck ack = PipelineAck.read(roundTrip(new PipelineAck(42, replies)::write));
        assertEquals(42, ack.seqno);
        assertArrayEquals(replies, ack.replies);
    }

    @Test
    void writeBlockHeaderRoundTrip() throws IOException {
        Block b = new Block(1073741830L, 0, 1007);
        DatanodeInfo[] targets = {
                new DatanodeInfo(new DatanodeID("u1", "localhost", 50011, 50021)),
                new DatanodeInfo(new DatanodeID("u2", "localhost", 50012, 50022))};
        DataInputStream in = roundTrip(out -> DataTransferProtocol.sendWriteBlock(out, b, "client-1", targets));
        assertEquals(DataTransferProtocol.DATA_TRANSFER_VERSION, in.readShort());
        assertEquals(DataTransferProtocol.OP_WRITE_BLOCK, in.readByte());
        Block read = DataTransferProtocol.readBlock(in);
        assertEquals(b, read);
        assertEquals(1007, read.getGenerationStamp());
        assertEquals("client-1", in.readUTF());
        assertEquals(2, in.readInt());
        assertEquals(targets[0], DataTransferProtocol.readDatanodeInfo(in));
        DatanodeInfo second = DataTransferProtocol.readDatanodeInfo(in);
        assertEquals(targets[1], second);
        assertEquals(50022, second.getIpcPort());
    }

    @Test
    void locatedBlockKeepsReferences() {
        Block b = new Block(1, 2, 3);
        LocatedBlock lb = new LocatedBlock(b, 10, new DatanodeInfo[0]);
        assertSame(b, lb.getBlock());
        assertEquals(10, lb.getStartOffset());
    }
}
