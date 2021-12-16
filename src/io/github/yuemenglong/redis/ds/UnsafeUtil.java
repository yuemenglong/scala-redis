//package com.nsn.redis.ds;
//
//import sun.misc.Unsafe;
//
//import java.nio.ByteBuffer;
//
//@SuppressWarnings({"PointlessBitwiseExpression", "PointlessArithmeticExpression"})
//public class UnsafeUtil {
//
//    private static Unsafe U = UnsafeAccess.U;
//
//    public static byte readByte(long addr, int pos) {
//        return U.getByte(addr + pos);
//    }
//
//    public static void writeByte(long addr, int pos, byte value) {
//        U.putByte(addr + pos, value);
//    }
//
//    public static short readShort(long addr, int pos) {
//        return (short) (((readByte(addr, pos + 1) & 0xFF) << 8) |
//                ((readByte(addr, pos + 0) & 0xFF) << 0));
//    }
//
//    public static void writeShort(long addr, int pos, short value) {
//        writeByte(addr, pos, (byte) ((value >> 0)));
//        writeByte(addr, pos + 1, (byte) ((value >> 8)));
//    }
//
//    public static int readInt(byte[] data, int pos) {
//        return ((data[pos + 3] & 0xFF) << 24) |
//                ((data[pos + 2] & 0xFF) << 16) |
//                ((data[pos + 1] & 0xFF) << 8) |
//                ((data[pos + 0] & 0xFF) << 0);
//    }
//
//    public static void writeInt(byte[] data, int pos, int value) {
//        data[pos] = (byte) (value >> 0);
//        data[pos + 1] = (byte) (value >> 8);
//        data[pos + 2] = (byte) (value >> 16);
//        data[pos + 3] = (byte) (value >> 24);
//    }
//
//    public static long readLong(byte[] data, int pos) {
//        return (((long) (data[pos + 7] & 0xFF) << 56) |
//                ((long) (data[pos + 6] & 0xFF) << 48) |
//                ((long) (data[pos + 5] & 0xFF) << 40) |
//                ((long) (data[pos + 4] & 0xFF) << 32) |
//                ((data[pos + 3] & 0xFF) << 24) |
//                ((data[pos + 2] & 0xFF) << 16) |
//                ((data[pos + 1] & 0xFF) << 8) |
//                ((data[pos + 0] & 0xFF) << 0));
//    }
//
//    public static void writeLong(byte[] data, int pos, long value) {
//        data[pos] = (byte) (value >> 0);
//        data[pos + 1] = (byte) (value >> 8);
//        data[pos + 2] = (byte) (value >> 16);
//        data[pos + 3] = (byte) (value >> 24);
//        data[pos + 4] = (byte) (value >> 32);
//        data[pos + 5] = (byte) (value >> 40);
//        data[pos + 6] = (byte) (value >> 48);
//        data[pos + 7] = (byte) (value >> 56);
//    }
//
//    public static void main(String[] args) {
////        byte[] data = new byte[8];
////        writeShort(data, 0, (short) 12345);
////        System.out.println(readShort(data, 0));
////        writeInt(data, 0, 12345678);
////        System.out.println(readInt(data, 0));
////        writeLong(data, 0, 12345678901234L);
////        System.out.println(readLong(data, 0));
////        ByteBuffer bb = ByteBuffer.allocateDirect(100);
//    }
//}
