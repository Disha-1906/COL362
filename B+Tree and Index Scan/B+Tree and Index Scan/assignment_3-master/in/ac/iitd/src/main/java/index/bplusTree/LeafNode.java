package index.bplusTree;

import java.nio.ByteBuffer;

/*
 * A LeafNode contains keys and block ids.
 * Looks Like -
 * # entries | prev leafnode | next leafnode | ptr to next free offset | blockid_1 | len(key_1) | key_1 ...
 *
 * Note: Only write code where specified!
 */
public class LeafNode<T> extends BlockNode implements TreeNode<T>{

    Class<T> typeClass;

    public LeafNode(Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        // set numEntries to 0
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = 0;
        numEntriesBytes[1] = 0;
        this.write_data(0, numEntriesBytes);

        // set ptr to next free offset to 8
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 8;
        this.write_data(6, nextFreeOffsetBytes);

        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        int offset = 10;
        for(int i=0;i<numKeys;i++){
            byte[] byte_length = this.get_data(offset,2);
            int int_length = ((byte_length[0] & 0xFF) << 8 ) | (byte_length[1] & 0xFF);
            byte[] byte_key = this.get_data(offset+2,int_length);
            keys[i] = convertBytesToT(byte_key, this.typeClass);
            offset = offset + 2 + int_length + 2;
        }

        return keys;

    }

    // returns the block ids in the node - will be evaluated
    public int[] getBlockIds() {

        int numKeys = getNumKeys();

        int[] block_ids = new int[numKeys];

        /* Write your code here */
        int offset = 8;
        for(int i=0;i<numKeys;i++){
            byte[] byte_block_id = this.get_data(offset,2);
            int int_block_id = ((byte_block_id[0] & 0xFF) << 8 ) | (byte_block_id[1] & 0xFF);
            byte[] byte_length = this.get_data(offset+2, 2);
            int int_length = ((byte_length[0] & 0xFF) << 8 ) | (byte_length[1] & 0xFF);
            offset = offset + 2 + 2 + int_length;
        }

        return block_ids;
    }



    private boolean greater(T key1, T key2){
        if( key1 instanceof Integer && key2 instanceof Integer){ return (Integer) key1 > (Integer) key2;}
        else if( key1 instanceof Float && key2 instanceof Float){ return (Float) key1 > (Float) key2;}
        else if( key1 instanceof Double && key2 instanceof Double){ return (Double) key1 > (Double) key2;}
        else if( key1 instanceof String && key2 instanceof String){ return ((String) key1).compareTo((String) key2) > 0;}
        else if( key1 instanceof Boolean && key2 instanceof Boolean){ return ((Boolean) key1 ? 1 : 0) > ((Boolean) key2 ? 1 : 0); }
        return false;
    }


    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int block_id) {

        /* Write your code here */
        T[] keys = getKeys();
        int[] block_ids = getBlockIds();
        byte[] byte_key = convertTtoBytes(key);
        int total_keys = getNumKeys();
        int i = 0;
        while (i < total_keys) {
            if (key == keys[i]) i++;
            else if (greater(key, keys[i])) i++;
            else break;
        }

        int off= 8;
        for(int j=0; j<i; j++){
            byte[] byte_length = this.get_data(off+2,2);
            int int_length = ((byte_length[0] & 0xFF) << 8) | (byte_length[1] & 0xFF);
            off = off + 4 + int_length;
        }

        byte[] block_id_bytes = new byte[2];
        block_id_bytes[0] = (byte) ((block_id >> 8) & 0xFF);
        block_id_bytes[1] = (byte) (block_id & 0xFF);
        this.write_data(off, block_id_bytes);
        byte[] byte_length = new byte[2];
        byte_length[0] = (byte) ((byte_key.length >> 8) & 0xFF);
        byte_length[1] = (byte) (byte_key.length & 0xFF);
        this.write_data(off+2, byte_length);


        this.write_data(off+4, byte_key);
        off = off + 4+ byte_key.length;

        for (int j = i; j<total_keys ; j++) {
            byte[] byte_block_id = new byte[2];
            byte_block_id[0] = (byte) ((block_ids[j] >> 8) & 0xFF);
            byte_block_id[1] = (byte) (block_ids[j] & 0xFF);
            this.write_data(off, block_id_bytes);

            byte[] byte_len = new byte[2];
            byte[] b_key = convertTtoBytes(keys[j]);
            int key_len = b_key.length;
            byte_len[0] = (byte) ((key_len >> 8) & 0xFF);
            byte_len[1] = (byte) (key_len & 0xFF);
            this.write_data(off + 2, byte_len);
            this.write_data(off+4, b_key);
            off += key_len + 4;
        }

        byte[] num_keys = new byte[2];
        num_keys[0] = (byte) (((total_keys+1) >> 8) & 0xFF);
        num_keys[1] = (byte) ((total_keys+1) & 0xFF);
        this.write_data(0, num_keys);

        byte[] next_free = new byte[2];
        next_free[0] = (byte) ((off >> 8) & 0xFF);
        next_free[1] = (byte) (off & 0xFF);
        this.write_data(6, next_free);
        return;

    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {

        /* Write your code here */

        int num_keys = getNumKeys();
        int offset = 8;

        for (int i = 0; i<num_keys; i++) {
            byte[] byte_block_id = this.get_data(offset, 2);
            int block_id = (byte_block_id[0] << 8) | (byte_block_id[1] & 0xFF);

            byte[] byte_len = this.get_data(offset+2, 2);
            int int_len = (byte_len[0] << 8) | (byte_len[1] & 0xFF);

            byte[] byte_key = this.get_data(offset, int_len);
            T key_0 = convertBytesToT(byte_key, this.typeClass);
            offset += int_len+4;

            if (key == key_0) {
                return block_id;
            }
        }

        return -1;
    }

}
