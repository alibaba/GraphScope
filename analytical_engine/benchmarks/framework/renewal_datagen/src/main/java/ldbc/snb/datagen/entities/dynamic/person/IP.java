package ldbc.snb.datagen.entities.dynamic.person;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IP implements Writable {

    public static final int BYTE_MASK = 0xFF;
    public static final int IP4_SIZE_BITS = 32;
    public static final int BYTE1_SHIFT_POSITION = 24;
    public static final int BYTE2_SHIFT_POSITION = 16;
    public static final int BYTE3_SHIFT_POSITION = 8;

    private int ip;
    private int mask;
    private int network;

    public IP() {

    }

    public IP(int byte1, int byte2, int byte3, int byte4, int networkMask) {
        ip = ((byte1 & BYTE_MASK) << BYTE1_SHIFT_POSITION) |
                ((byte2 & BYTE_MASK) << BYTE2_SHIFT_POSITION) |
                ((byte3 & BYTE_MASK) << BYTE3_SHIFT_POSITION) |
                (byte4 & BYTE_MASK);

        mask = 0xFFFFFFFF << (IP4_SIZE_BITS - networkMask);
        network = ip & mask;
    }

    public IP(IP i) {
        this.ip = i.ip;
        this.mask = i.mask;
        this.network = i.network;
    }

    public IP(int ip, int mask) {
        this.ip = ip;
        this.mask = mask;
        this.network = this.ip & this.mask;
    }

    public int getIp() {
        return ip;
    }

    public int getMask() {
        return mask;
    }


    public int getNetwork() {
        return network;
    }


    public String toString() {
        return ((ip >>> BYTE1_SHIFT_POSITION) & BYTE_MASK) + "." +
                ((ip >>> BYTE2_SHIFT_POSITION) & BYTE_MASK) + "." +
                ((ip >>> BYTE3_SHIFT_POSITION) & BYTE_MASK) + "." +
                (ip & BYTE_MASK);
    }

    @Override
    public boolean equals(Object obj) {
        IP a = (IP) obj;
        return this.ip == a.ip && this.mask == a.mask;
    }

    public void copy(IP ip) {
        this.ip = ip.ip;
        this.mask = ip.mask;
        this.network = ip.network;
    }

    public void readFields(DataInput arg0) throws IOException {
        ip = arg0.readInt();
        mask = arg0.readInt();
        network = arg0.readInt();
    }

    public void write(DataOutput arg0) throws IOException {
        arg0.writeInt(ip);
        arg0.writeInt(mask);
        arg0.writeInt(network);
    }

    public int hashCode() {
        return super.hashCode();
    }
}
