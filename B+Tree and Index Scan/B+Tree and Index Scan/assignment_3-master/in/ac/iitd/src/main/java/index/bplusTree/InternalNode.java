package index.bplusTree;

/*
 * Internal Node - num Keys | ptr to next free offset | P_1 | len(K_1) | K_1 | P_2 | len(K_2) | K_2 | ... | P_n
 * Only write code where specified

 * Remember that each Node is a block in the Index file, thus, P_i is the block_id of the child node
 */
public class InternalNode<T> extends BlockNode implements TreeNode<T> {

    // Class of the key
    Class<T> typeClass;

    // Constructor - expects the key, left and right child ids
    public InternalNode(T key, int left_child_id, int right_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) ((left_child_id >> 8) & 0xFF);
        child_1[1] = (byte) (left_child_id & 0xFF);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        // also calls the insert method
        this.insert(key, right_child_id);
        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        int offset = 6;
        for (int i = 0; i<numKeys; i++) {
            byte[] byte_len = this.get_data(offset, 2);
            int int_len = ((byte_len[0] & 0xFF) << 8) | (byte_len[1] & 0xFF);
            byte[] byte_key = this.get_data(offset+2, int_len);
            keys[i] = convertBytesToT(byte_key, this.typeClass);
            offset += int_len + 4;
        }

        return keys;
    }

    private int getOffset_for_newNode(int j) {
        int offset = 6;

        for (int i = 0; i < j; i++) {
            byte[] key_len_bytes = this.get_data(offset, 2);
            int key_len = ((key_len_bytes[0] & 0xFF) << 8) | (key_len_bytes[1] & 0xFF);
            offset += 2 + key_len + 2;
        }
        return offset;
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
        int[] children = getChildren();
        byte[] byte_key = convertTtoBytes(key);
        int total_keys = getNumKeys();

        int i = 0;
        while (i < total_keys) {
            if (key == keys[i]) {i++;}
            else if (greater(key, keys[i])) { i++;}
            else { break;}
        }

//        int offset = getOffset_for_newNode(i);
        int off= 6;
        for(int j=0; j<i; j++){
            byte[] byte_length = this.get_data(off,2);
            int int_length = ((byte_length[0] & 0xFF) << 8) | (byte_length[1] & 0xFF);
            off = off + 4 + int_length;
        }
//        System.out.println("--------------------------KEY LENGTH----------IS-----  " + byte_key.length +"\n");

        byte[] byte_length = new byte[2];
        byte_length[0] = (byte) ((byte_key.length >> 8) & 0xFF);
        byte_length[1] = (byte) (byte_key.length & 0xFF);
        this.write_data(off, byte_length);
        this.write_data(off+2, byte_key);
        byte[] block_id_bytes = new byte[2];
        block_id_bytes[0] = (byte) ((block_id >> 8) & 0xFF);
        block_id_bytes[1] = (byte) (block_id & 0xFF);
        this.write_data(off+2+byte_key.length, block_id_bytes);
        off = off + 4 + byte_key.length;

        for (int j = i; j < total_keys; j++) {
            byte[] byte_len = new byte[2];
//            byte[] keyBytes = keys[j].toString().getBytes();
            byte[] byte_key_1 = convertTtoBytes(keys[j]);
            int key_length = byte_key.length;
            byte_len[0] = (byte) ((key_length >> 8) & 0xFF);
            byte_len[1] = (byte) (key_length & 0xFF);
            this.write_data(off, byte_len);

            this.write_data(off+2, byte_key_1);
            byte[] byte_child = new byte[2];
            byte_child[0] = (byte) (children[j+1] >> 8 & 0xFF);
            byte_child[1] = (byte) (children[j+1] & 0xFF);
            this.write_data(off+2+key_length, byte_child);
            off += 4+key_length;
        }

        byte[] num_keys = new byte[2];
        num_keys[0] = (byte) (((total_keys+1) >> 8) & 0xFF);
        num_keys[1] = (byte) ((total_keys+1) & 0xFF);
        this.write_data(0, num_keys);

        byte[] next_free = new byte[2];
        next_free[0] = (byte) ((off >> 8) & 0xFF);
        next_free[1] = (byte) (off & 0xFF);
        this.write_data(2, next_free);
        return;

    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {
        /* Write your code here */

        T[] keys = this.getKeys();
        int total_keys = this.getNumKeys();
        int i=0;
        while(i<total_keys){
            if(key==keys[i]) return i;
            else if(greater(key,keys[i])) i++;
            else return -1;
        }
        return -1;
    }

    // should return the block_ids of the children - will be evaluated
    public int[] getChildren() {

        byte[] numKeysBytes = this.get_data(0, 2);
        int numKeys = ((numKeysBytes[0] & 0xFF) << 8) | (numKeysBytes[1] & 0xFF);

        int[] children = new int[numKeys + 1];

        /* Write your code here */
        int off = 4;
        for(int i=0;i<=numKeys;i++){
            byte[] b_child = this.get_data(off,2);
            byte[] b_len = this.get_data(off+2,2);
            children[i] = ((b_child[0] & 0xFF) << 8) | (b_child[1] & 0xFF);
            int i_len = ((b_len[0] & 0xFF) << 8) | (b_len[1] & 0xFF);
            off += 4+i_len;
        }

        return children;

    }

}
