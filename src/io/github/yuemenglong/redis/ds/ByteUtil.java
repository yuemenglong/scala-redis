package io.github.yuemenglong.redis.ds;

@SuppressWarnings({"PointlessBitwiseExpression", "PointlessArithmeticExpression"})
public class ByteUtil {
    public static final int BYTE_SIZE = 1;
    public static final int SHORT_SIZE = 2;
    public static final int INT_SIZE = 4;
    public static final int LONG_SIZE = 8;

    public static short readShort(byte[] data, int pos) {
        return (short) (((data[pos + 1] & 0xFF) << 8) |
                ((data[pos + 0] & 0xFF) << 0));
    }

    public static void writeShort(byte[] data, int pos, short value) {
        data[pos] = (byte) ((value >> 0));
        data[pos + 1] = (byte) ((value >> 8));
    }

    public static int readInt(byte[] data, int pos) {
        return ((data[pos + 3] & 0xFF) << 24) |
                ((data[pos + 2] & 0xFF) << 16) |
                ((data[pos + 1] & 0xFF) << 8) |
                ((data[pos + 0] & 0xFF) << 0);
    }

    public static void writeInt(byte[] data, int pos, int value) {
        data[pos] = (byte) (value >> 0);
        data[pos + 1] = (byte) (value >> 8);
        data[pos + 2] = (byte) (value >> 16);
        data[pos + 3] = (byte) (value >> 24);
    }

    public static long readLong(byte[] data, int pos) {
        return (((long) (data[pos + 7] & 0xFF) << 56) |
                ((long) (data[pos + 6] & 0xFF) << 48) |
                ((long) (data[pos + 5] & 0xFF) << 40) |
                ((long) (data[pos + 4] & 0xFF) << 32) |
                ((data[pos + 3] & 0xFF) << 24) |
                ((data[pos + 2] & 0xFF) << 16) |
                ((data[pos + 1] & 0xFF) << 8) |
                ((data[pos + 0] & 0xFF) << 0));
    }

    public static void writeLong(byte[] data, int pos, long value) {
        data[pos] = (byte) (value >> 0);
        data[pos + 1] = (byte) (value >> 8);
        data[pos + 2] = (byte) (value >> 16);
        data[pos + 3] = (byte) (value >> 24);
        data[pos + 4] = (byte) (value >> 32);
        data[pos + 5] = (byte) (value >> 40);
        data[pos + 6] = (byte) (value >> 48);
        data[pos + 7] = (byte) (value >> 56);
    }

    public static void main(String[] args) {
        byte[] data = new byte[8];
        writeShort(data, 0, (short) 12345);
        System.out.println(readShort(data, 0));
        writeInt(data, 0, 12345678);
        System.out.println(readInt(data, 0));
        writeLong(data, 0, 12345678901234L);
        System.out.println(readLong(data, 0));
    }
}
