package index.bplusTree;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Comparator;

// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();

    public void insert(T key, int block_id);

    public int search(T key);

    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }

    // Might be useful for you - will not be evaluated
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass) {

        /* Write your code here */

        if (bytes == null || bytes.length == 0) { return null; }
        else if (typeClass == String.class) { return typeClass.cast(new String(bytes));}
        else if (typeClass == Boolean.class) {
            boolean val = bytes[0] != 0;
            return typeClass.cast(val);
        }
        else if (typeClass == Integer.class){
            int val = ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
            return typeClass.cast(val);
        }
        else if (typeClass == Float.class) {
            int int_val = ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
            float val = Float.intBitsToFloat(int_val);
            return typeClass.cast(val);
        }
        else if (typeClass == Double.class){
            long int_val = ((bytes[0] & 0xFFL) << 56) | ((bytes[1] & 0xFFL) << 48) | ((bytes[2] & 0xFFL) << 40) |
                    ((bytes[3] & 0xFFL) << 32) | ((bytes[4] & 0xFFL) << 24) | ((bytes[5] & 0xFFL) << 16) |
                    ((bytes[6] & 0xFFL) << 8) | (bytes[7] & 0xFFL);
            double val = Double.longBitsToDouble(int_val);
            return typeClass.cast(val);
        }
        throw new IllegalArgumentException("Unsupported type for conversion");
//        return null;


    }

    default public byte[] convertTtoBytes(T key) {
        if(key==null){ return null; }
        else if( key instanceof String){ return ((String) key).getBytes();}
        else if( key instanceof Integer) {
            int int_key = (Integer) key;
            byte[] byte_key = new byte[4];
            byte_key[0] = (byte) (int_key >> 24);
            byte_key[1] = (byte) (int_key >> 16);
            byte_key[2] = (byte) (int_key >> 8);
            byte_key[3] = (byte) (int_key);
            return byte_key;
        }
        else if( key instanceof Boolean){
            byte byte_key = (byte) (((Boolean) key) ? 1 : 0);
            return new byte[]{byte_key};
        }
        else if( key instanceof Float) {
            int float_key = Float.floatToIntBits((Float) key);
            byte[] byte_key = new byte[4];
            byte_key[0] = (byte) (float_key >> 24);
            byte_key[1] = (byte) (float_key >> 16);
            byte_key[2] = (byte) (float_key >> 8);
            byte_key[3] = (byte) (float_key);
            return byte_key;
        }
        else if( key instanceof Double){
            long double_key = Double.doubleToLongBits((Double) key);
            byte[] byte_key = new byte[8];
            byte_key[0] = (byte) (double_key >> 56);
            byte_key[1] = (byte) (double_key >> 48);
            byte_key[2] = (byte) (double_key >> 40);
            byte_key[3] = (byte) (double_key >> 32);
            byte_key[4] = (byte) (double_key >> 24);
            byte_key[5] = (byte) (double_key >> 16);
            byte_key[6] = (byte) (double_key >> 8);
            byte_key[7] = (byte) (double_key);
            return byte_key;
        }
        throw new IllegalArgumentException("Unsupported type for conversion");
    }
}



