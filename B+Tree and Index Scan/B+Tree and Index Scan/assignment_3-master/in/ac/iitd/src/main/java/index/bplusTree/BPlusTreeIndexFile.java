package index.bplusTree;

import storage.AbstractFile;

import java.util.Queue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Stack;

/*
 * Tree is a collection of BlockNodes
 * The first BlockNode is the metadata block - stores the order and the block_id of the root node

 * The total number of keys in all leaf nodes is the total number of records in the records file.
 */

public class BPlusTreeIndexFile<T> extends AbstractFile<BlockNode> {

    Class<T> typeClass;

    // Constructor - creates the metadata block and the root node
    public BPlusTreeIndexFile(int order, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;
        BlockNode node = new BlockNode(); // the metadata block
        LeafNode<T> root = new LeafNode<>(typeClass);

        // 1st 2 bytes in metadata block is order
        byte[] orderBytes = new byte[2];
        orderBytes[0] = (byte) (order >> 8);
        orderBytes[1] = (byte) order;
        node.write_data(0, orderBytes);

        // next 2 bytes are for root_node_id, here 1
        byte[] rootNodeIdBytes = new byte[2];
        rootNodeIdBytes[0] = 0;
        rootNodeIdBytes[1] = 1;
        node.write_data(2, rootNodeIdBytes);

        // push these nodes to the blocks list
        blocks.add(node);
        blocks.add(root);
    }

    private boolean isFull(int id){
        // 0th block is metadata block
        assert(id > 0);
        return blocks.get(id).getNumKeys() == getOrder() - 1;
    }

    private int getRootId() {
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
//        return ((rootBlockIdBytes[0] << 8) & 0xF0) | (rootBlockIdBytes[1] & 0x0F);
        return ((rootBlockIdBytes[0] << 8) & 0xFF) | (rootBlockIdBytes[1] & 0xFF);
    }

    public int getOrder() {
        BlockNode node = blocks.get(0);
        byte[] orderBytes = node.get_data(0, 2);
        return ((orderBytes[0] << 8) & 0xF0) | (orderBytes[1] & 0x0F);
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id){
        BlockNode node = blocks.get(id);
        return isLeaf(blocks.get(id));
    }

    private boolean greater(T key1, T key2){
        if( key1 instanceof Integer && key2 instanceof Integer){ return (Integer) key1 > (Integer) key2;}
        else if( key1 instanceof Float && key2 instanceof Float){ return (Float) key1 > (Float) key2;}
        else if( key1 instanceof Double && key2 instanceof Double){ return (Double) key1 > (Double) key2;}
        else if( key1 instanceof String && key2 instanceof String){ return ((String) key1).compareTo((String) key2) > 0;}
        else if( key1 instanceof Boolean && key2 instanceof Boolean){ return ((Boolean) key1 ? 1 : 0) > ((Boolean) key2 ? 1 : 0); }
        return false;
    }


    private int get_position(T key, T[] arr){
        int i =0;
        while(i<arr.length){
            if(key==arr[i]) break;
            else if(greater(key,arr[i])) i++;
            else break;
        }
        return i;
    }

    private void reset_leaf( LeafNode<T> node){
        byte[] next_free = new byte[2];
        next_free[0] = 0;
        next_free[1] = 8;
        node.write_data(6, next_free);

        byte[] num_keys = new byte[2];
        num_keys[0] = (byte) (0);
        num_keys[1] = (byte) (0);
        node.write_data(0, num_keys);
    }

    private void reset_node( InternalNode<T> node){
        byte[] next_free = new byte[2];
        next_free[0] = 0;
        next_free[1] = 6;
        node.write_data(2, next_free);

        byte[] num_keys = new byte[2];
        num_keys[0] = (byte) (0);
        num_keys[1] = (byte) (0);
        node.write_data(0, num_keys);
    }

    private void update_root_id(int id){
        byte[] b_id = new byte [2];
        BlockNode b = blocks.get(0);
        b_id[0] = (byte) (((blocks.size()-1) >> 8) & 0xFF);
        b_id[1] = (byte) ((blocks.size()-1) & 0xFF);
        b.write_data(2,b_id);
    }

    private void update(T[] arr, int[] ids, T[] new_arr, int[] new_ids, int pos, int m, T key, int block_id, boolean b){
        for(int i=0;i<m;i++){
            if(i<pos){
                new_arr[i] = arr[i];
                if(b) new_ids[i] = ids[i];
                else new_ids[i+1] = ids[i+1];
            }
            else if(i==pos){
                new_arr[i] = key;
                if(b) new_ids[i] = block_id;
                else new_ids[i+1] = block_id;
            }
            else{
                new_arr[i] = arr[i-1];
                if(b) new_ids[i] = ids[i-1];
                else new_ids[i+1] =  ids[i];
            }
        }
    }
    // will be evaluated
    public void insert(T key, int block_id) {

        /* Write your code here */

        if (this != null) {
            if (key == null) return;
            int root_id = getRootId();
            int m = getOrder();
            if (isLeaf(root_id)) {
                LeafNode<T> root = (LeafNode<T>) blocks.get(root_id);
                if (!isFull(root_id)) {
                    root.insert(key, block_id);
                } else{
                    T[] root_keys = root.getKeys();
                    int[] block_ids = root.getBlockIds();
                    int pos = get_position(key, root_keys);
                    T[] new_keys = (T[]) new Object[m];
                    int[] new_ids = new int[m];
                    update(root_keys, block_ids, new_keys, new_ids, pos, m, key, block_id, true);
                    reset_leaf(root);
                    LeafNode<T> right_leaf = new LeafNode<>(typeClass);
                    blocks.add(right_leaf);
                    blocks.set(blocks.size() - 1, right_leaf);
                    int right_leaf_id = blocks.size() - 1;
                    T new_root_key = null;
                    for (int i = 0; i < m; i++) {
                        if (i < m / 2) root.insert(new_keys[i], new_ids[i]);
                        if (i == m / 2) new_root_key = new_keys[i];
                        if (i >= m / 2) right_leaf.insert(new_keys[i], new_ids[i]);
                    }
                    InternalNode<T> new_root = new InternalNode<>(new_root_key, root_id, right_leaf_id, this.typeClass);
                    blocks.add(new_root);
                    blocks.set(blocks.size() - 1, new_root);
                    update_root_id(blocks.size() - 1);
                }
            } else {
                int id = root_id;
                Stack<Integer> path = new Stack<>();
                while (!isLeaf(id)) {
                    path.push(id);
                    InternalNode<T> i_node = (InternalNode<T>) blocks.get(id);
                    T[] i_node_keys = i_node.getKeys();
                    int[] i_node_children = i_node.getChildren();
                    int pos = get_position(key, i_node_keys);
                    id = i_node_children[pos];
                }
                LeafNode<T> leaf = (LeafNode<T>) blocks.get(id);
                if (!isFull(id)) {
                    leaf.insert(key, block_id);
                } else {
                    T[] keys = leaf.getKeys();
                    int[] block_ids = leaf.getBlockIds();
                    int pos = get_position(key, keys);

                    T[] new_keys = (T[]) new Object[m];
                    int[] new_ids = new int[m];
                    update(keys, block_ids, new_keys, new_ids, pos, m, key, block_id, true);
                    reset_leaf(leaf);
                    LeafNode<T> right_leaf = new LeafNode<>(typeClass);
                    blocks.add(right_leaf);
                    blocks.set(blocks.size() - 1, right_leaf);
                    int right_leaf_id = blocks.size() - 1;
                    T new_root_key = null;
                    for (int i = 0; i < m; i++) {
                        if (i < m / 2) leaf.insert(new_keys[i], new_ids[i]);
                        if (i == m / 2) new_root_key = new_keys[i];
                        if (i >= m / 2) right_leaf.insert(new_keys[i], new_ids[i]);
                    }
                    while (path.isEmpty() == false) {
                        int pop_id = path.pop();
                        InternalNode<T> inode = (InternalNode<T>) blocks.get(pop_id);
                        if (isFull(pop_id)) {
                            T[] pop_keys = inode.getKeys();
                            int[] pop_children = inode.getChildren();
                            int pop_pos =  get_position(new_root_key, pop_keys);
                            T[] new_pop_keys = (T[]) new Object[m];
                            int[] new_pop_children = new int[m+1];
                            new_pop_children[0] = pop_children[0];
                            update(pop_keys, pop_children, new_pop_keys, new_pop_children, pop_pos, m, new_root_key, right_leaf_id, false);
                            reset_node(inode);
                            byte[] child0 = new byte[2];
                            child0[0] = (byte) ((new_pop_children[0] >> 8) & 0xFF);
                            child0[1] = (byte) (new_pop_children[0] & 0xFF);
                            inode.write_data(4, child0);
                            int y = 0;
                            while (y < m/2) {
                                inode.insert(new_pop_keys[y], new_pop_children[y+1]);
                                y++;
                            }
                            new_root_key = new_pop_keys[y];
                            y++;
                            InternalNode<T> right_inode = new InternalNode<>(new_pop_keys[y], new_pop_children[y], new_pop_children[y+1], this.typeClass);
                            blocks.add(right_inode);
                            blocks.set(blocks.size() - 1, right_inode);
                            y++;
                            while (y < m) {
                                right_inode.insert(new_pop_keys[y], new_pop_children[y+1]);
                                y++;
                            }
                            right_leaf_id = blocks.size()-1;

                            if (path.isEmpty()) {
                                InternalNode<T> new_root = new InternalNode<>(new_root_key, root_id, right_leaf_id, this.typeClass);
                                blocks.add(new_root);
                                blocks.set(blocks.size()-1, new_root);
                                update_root_id(blocks.size() - 1);
                            }

                        } else {
                            inode.insert(new_root_key, right_leaf_id);
                            break;
                        }
                    }

                }
            }
        }
        return;
    }

    // will be evaluated
    // returns the block_id of the leftmost leaf node containing the key
    public int search(T key) {

        /* Write your code here */
        if(this!=null){
            if(key == null) return -1;
            int id = getRootId();
            boolean flag = true;
            while(!isLeaf(id)){
                InternalNode<T> inode = (InternalNode<T>) blocks.get(id);
                T[] inode_keys = inode.getKeys();
                int[] children = inode.getChildren();
                int i = 0;
                while(i<inode_keys.length){
                    if(key==inode_keys[i]){break;}
                    else if(greater(key,inode_keys[i])) i++;
                    else break;
                }
                if(i!=inode_keys.length) flag = false;
                id = children[i];
            }
            LeafNode<T> leaf = (LeafNode<T>) blocks.get(id);
            T[] leaf_keys = leaf.getKeys();
            if(flag ==true && greater(key, leaf_keys[leaf_keys.length -1]) ) return -1;
            else return id;
        }
        return -1;
    }

    // returns true if the key was found and deleted, false otherwise
    // (Optional for Assignment 3)
    public boolean delete(T key) {

        /* Write your code here */
        return false;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public void print_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                BlockNode b = blocks.get(id);
                ((LeafNode<T>) blocks.get(id)).print();
            }
            else {
                ((InternalNode<T>) blocks.get(id)).print();
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public ArrayList<T> return_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }

                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
//                    if (children[i] != 65535) {
                    queue.add(children[i]);
//                    }
                }
            }
        }
        return bfs;
    }

    public void print() {
        print_bfs();
        return;
    }

}