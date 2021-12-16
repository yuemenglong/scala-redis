package io.github.yuemenglong.redis.ds;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ByteArrayDeque implements Deque<byte[]> {

    private byte[] buf = new byte[0];
    private int head = 0;
    private int tail = 0;
    private int count = 0;

    private int pos(int n) {
        if (buf.length == 0) {
            throw new RuntimeException("Unreachable");
        }
        return (n + buf.length) % buf.length;
    }

    public int used() {
        if (buf.length == 0) {
            return 0;
        }
        return (tail - head + buf.length) % buf.length;
    }

    public int remaining() {
        return buf.length - used();
    }

    public int capacity() {
        return buf.length;
    }

    private void ensureCapacity(int min) {
        if (remaining() < min) {
            grow(used() + min);
        }
    }

    private void grow(int min) {
        int oldSize = buf.length;
        int newSize = oldSize + (oldSize >> 1);
        if (newSize < min) {
            newSize = min;
        }
        byte[] newBuf = Arrays.copyOf(buf, newSize);
        if (tail < head) {
            // 将head的数据移动到尾部
            int l = buf.length - head;
            System.arraycopy(buf, head, newBuf, newSize - l, l);
            head = newSize - l;
        }
        buf = newBuf;
    }

    @SuppressWarnings({"PointlessArithmeticExpression", "PointlessBitwiseExpression"})
    private void writeInt(int p, int n) {
        buf[pos(p + 0)] = (byte) (n >> 24);
        buf[pos(p + 1)] = (byte) (n >> 16);
        buf[pos(p + 2)] = (byte) (n >> 8);
        buf[pos(p + 3)] = (byte) (n >> 0);
    }

    @SuppressWarnings({"PointlessBitwiseExpression", "PointlessArithmeticExpression"})
    private int readInt(int p) {
        return (buf[pos(p + 0)] & 0xFF) << 24 |
                (buf[pos(p + 1)] & 0xFF) << 16 |
                (buf[pos(p + 2)] & 0xFF) << 8 |
                (buf[pos(p + 3)] & 0xFF) << 0;
    }

    private void writeBytes(int p0, byte[] data) {
        int p = pos(p0);
        int end = p + data.length;
        if (end > buf.length) {
            end = buf.length;
        }
        int len1 = end - p;
        System.arraycopy(data, 0, buf, p, len1);
        if (len1 < data.length) {
            int start = len1;
            int len2 = data.length - len1;
            System.arraycopy(data, start, buf, 0, len2);
        }
    }

    private byte[] readBytes(int p0, int len) {
        int p = pos(p0);
        byte[] data = new byte[len];
        int end = p + data.length;
        if (end > buf.length) {
            end = buf.length;
        }
        int len1 = end - p;
        System.arraycopy(buf, p, data, 0, len1);
        if (len1 < data.length) {
            int start = len1;
            int len2 = data.length - len1;
            System.arraycopy(buf, 0, data, start, len2);
        }
        return data;
    }

    private void writeData(int p, byte[] data) {
        // 保证两边都能读到
        writeInt(p, data.length);
        writeBytes(p + 4, data);
        writeInt(p + 4 + data.length, -data.length);
    }

    private byte[] readData(int p) {
        int len = readInt(p);
        if (len == 0) {
            return new byte[0];
        }
        if (len > 0) {
            return readBytes(p + 4, len);
        } else {
            len = -len;
            return readBytes(p - len, len);
        }
    }


    @Override
    public void addFirst(byte[] bytes) {
        // 保证头尾不会相遇
        ensureCapacity(bytes.length + 9);
        writeData(head - bytes.length - 8, bytes);
        head = pos(head - bytes.length - 8);
        count += 1;
    }

    @Override
    public void addLast(byte[] bytes) {
        // 保证头尾不会相遇
        ensureCapacity(bytes.length + 9);
        writeData(tail, bytes);
        tail = pos(tail + bytes.length + 8);
        count += 1;
    }

    @Override
    public boolean offerFirst(byte[] bytes) {
        addFirst(bytes);
        return true;
    }

    @Override
    public boolean offerLast(byte[] bytes) {
        addLast(bytes);
        return true;
    }

    @Override
    public byte[] removeFirst() {
        if (count == 0) {
            return null;
        }
        byte[] data = readData(head);
        count -= 1;
        head = pos(head + data.length + 8);
        return data;
    }

    @Override
    public byte[] removeLast() {
        if (count == 0) {
            return null;
        }
        byte[] data = readData(tail - 4);
        count -= 1;
        tail = pos(tail - data.length - 8);
        return data;
    }

    @Override
    public byte[] pollFirst() {
        return removeFirst();
    }

    @Override
    public byte[] pollLast() {
        return removeLast();
    }

    @Override
    public byte[] getFirst() {
        if (count == 0) {
            return null;
        }
        return readData(head);
    }

    @Override
    public byte[] getLast() {
        if (count == 0) {
            return null;
        }
        return readData(tail - 4);
    }

    @Override
    public byte[] peekFirst() {
        return getFirst();
    }

    @Override
    public byte[] peekLast() {
        return getLast();
    }

    @Override
    public boolean removeFirstOccurrence(Object o) {
        throw new RuntimeException("Unimplemented");
    }

    @Override
    public boolean removeLastOccurrence(Object o) {
        throw new RuntimeException("Unimplemented");
    }

    @Override
    public boolean add(byte[] bytes) {
        addLast(bytes);
        return true;
    }

    @Override
    public boolean offer(byte[] bytes) {
        return add(bytes);
    }

    @Override
    public byte[] remove() {
        return removeLast();
    }

    @Override
    public byte[] poll() {
        return remove();
    }

    @Override
    public byte[] element() {
        return getLast();
    }

    @Override
    public byte[] peek() {
        return element();
    }

    @Override
    public void push(byte[] bytes) {
        add(bytes);
    }

    @Override
    public byte[] pop() {
        return remove();
    }

    @Override
    public boolean remove(Object o) {
        throw new RuntimeException("Unimplemented");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new RuntimeException("Unimplemented");
    }

    @Override
    public boolean addAll(Collection<? extends byte[]> c) {
        throw new RuntimeException("Unimplemented");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new RuntimeException("Unimplemented");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new RuntimeException("Unimplemented");
    }

    @Override
    public void clear() {
        buf = new byte[0];
        head = 0;
        tail = 0;
        count = 0;
    }

    @Override
    public boolean contains(Object o) {
        throw new RuntimeException("Unimplemented");
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Iterator<byte[]> iterator() {
        final AtomicInteger p = new AtomicInteger(head);
        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return p.get() != tail;
            }

            @Override
            public byte[] next() {
                int len = readInt(p.get());
                byte[] data = readBytes(p.get() + 4, len);
                p.set(pos(p.get() + 4 + len + 4));
                return data;
            }
        };
    }

    @Override
    public Object[] toArray() {
        ArrayList<byte[]> ret = new ArrayList<>(this);
        return ret.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        ArrayList<byte[]> ret = new ArrayList<>(this);
        return ret.toArray(a);
    }

    @Override
    public Iterator<byte[]> descendingIterator() {
        final AtomicInteger p = new AtomicInteger(tail);
        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return p.get() != head;
            }

            @Override
            public byte[] next() {
                int len = -readInt(p.get() - 4);
                byte[] data = readBytes(p.get() - 4 - len, len);
                p.set(pos(p.get() - 4 - len - 4));
                return data;
            }
        };
    }

    @Override
    public String toString() {
        if (head < tail) {
            return String.format("[0]____[%d]########[%d]____[%d]", head, tail, buf.length - 1);
        } else if (head > tail) {
            return String.format("[0]####[%d]________[%d]####[%d]", tail, head, buf.length - 1);
        } else {
            return String.format("[0]____[%d]____[%d]", head, buf.length - 1);
        }
    }

    public static void main(String[] args) throws IOException {
        ByteArrayDeque q = new ByteArrayDeque();
        q.grow(100);
        q.writeInt(0, -500);
        int r = q.readInt(0);
        System.out.println(r);
    }
}
