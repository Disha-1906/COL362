package manager;

import index.bplusTree.BPlusTreeIndexFile;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import storage.DB;
import storage.File;
import storage.Block;
import Utils.CsvRowConverter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Iterator;

public class StorageManager {

    private HashMap<String, Integer> file_to_fileid;
    private DB db;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    public StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        System.out.println("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        assert(file_to_fileid.get(table_name) == null);

        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();

                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }

                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte array
                byte[] record = convertToByteArray(row, typeList);

                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("Done writing file\n");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }

    // converts a row to byte array to write to relational file
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                } else {
                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {         
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                System.out.println("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }
        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0 ; i < columnNames.size() ; i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF); 
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }

    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public boolean check_index_exists(String table_name, String column_name) {
        String index_file_name = table_name + "_" + column_name + "_index";
        return file_to_fileid.get(index_file_name) != null;
    }

    private boolean check_null_bitmap(byte[] null_bitmap, int i){
        int b1 = i/8;
        int b2 = i%8;
        byte mask = (byte) (1<<b2);
        boolean val = (null_bitmap[b1] & mask) != 0;
        return val;
    }

    private Object[] convertToObjArr(byte[] record, String table_name){
        Block record_block = new Block(record);
        List<Object> objects = new ArrayList<>();
        int file_id = file_to_fileid.get(table_name);
        byte[] schema = db.get_data(file_id,0);
        int total_cols = ( schema[0] & 0xFF) | ((schema[1] & 0xFF) <<8);
        int fixed_cols = 0;
        int var_cols = 0;
        int null_offset = 0;
        int j = 2;
        Block s_block = new Block(schema);
        int last_offset = s_block.get_block_capacity()-1;
        List<RelDataType> typeList = new ArrayList<>();
        RelDataTypeFactory type_factory = new JavaTypeFactoryImpl();
        for(int i=0;i<total_cols;i++){
            int col_offset =  ((schema[j+1] & 0xFF) <<8) | (schema[j] & 0xFF);
            j+=2;
            byte[] col = s_block.get_data(col_offset, last_offset - col_offset);
            int datatype = col[0]& 0xFF;
            switch(datatype){
                case 0 : var_cols += 1;
                    typeList.add(type_factory.createSqlType(SqlTypeName.VARCHAR));
                    break;
                case 1 : fixed_cols += 1;
                    null_offset += 4;
                    typeList.add(type_factory.createSqlType(SqlTypeName.INTEGER));
                    break;
                case 2 : fixed_cols += 1;
                    null_offset += 1;
                    typeList.add(type_factory.createSqlType(SqlTypeName.BOOLEAN));
                    break;
                case 3 : fixed_cols += 1;
                    null_offset += 4;
                    typeList.add(type_factory.createSqlType(SqlTypeName.FLOAT));
                    break;
                case 4 : fixed_cols += 1;
                    null_offset += 8;
                    typeList.add(type_factory.createSqlType((SqlTypeName.DOUBLE)));
                    break;
            }
            last_offset = col_offset;
        }
//        System.out.print("Loop 1 done\n");
        int index = 4 * var_cols;
        int null_off = index + null_offset;
        int length = (fixed_cols + var_cols + 7)/8;
        byte[] null_bitmap = s_block.get_data(null_off, length);
        for(int i=0; i<fixed_cols; i++){
            RelDataType type = typeList.get(i);
            boolean check_null = check_null_bitmap(null_bitmap,i);
            RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
            if(type == type_factory.createSqlType(SqlTypeName.INTEGER) ){
                if(check_null) objects.add(null);
                else{
//                    System.out.print("Inside Integer\n");
//                    System.out.print("Records size " + record.length + "index "+ index+ "\n");
                    int val = 0;
                    val = ((record[index+3] & 0xFF) << 24) | ((record[index + 2] & 0xFF) << 16) | ((record[index + 1] & 0xFF) << 8) | (record[index ] & 0xFF);
                    objects.add(val);
                }
//                System.out.print("Outside Integer\n");
                index += 4;
            }
            else if(type == type_factory.createSqlType(SqlTypeName.BOOLEAN)){
                if(check_null) objects.add(null);
                else{
                    boolean val;
                    val = (record[index]) !=0;
                    objects.add(val);
                }
                index += 1;
            }
            else if(type == type_factory.createSqlType(SqlTypeName.FLOAT)){
                if(check_null) objects.add(null);
                else{
                    int intval = 0;
                    intval = ((record[index+3] & 0xFF) << 24) | ((record[index + 2] & 0xFF) << 16) | ((record[index + 1] & 0xFF) << 8) | (record[index ] & 0xFF);
                    float val = Float.intBitsToFloat(intval);
                    objects.add(val);
                }
                index += 4;
            } else if (type == type_factory.createSqlType(SqlTypeName.DOUBLE)) {
                if(check_null) objects.add(null);
                else{
                    long intval = 0;
                    intval = ((record[index + 7] & 0xFFL) << 56) | ((record[index + 6] & 0xFFL) << 48) | ((record[index + 5] & 0xFFL) << 40) |
                            ((record[index + 4] & 0xFFL) << 32) | ((record[index + 3] & 0xFFL) << 24) | ((record[index + 2] & 0xFFL) << 16) |
                            ((record[index + 1] & 0xFFL) << 8) | (record[index] & 0xFFL);
                    double val = Double.longBitsToDouble(intval);
                    objects.add(val);
                }
                index += 8;
            }
        }
//        System.out.print("Loop 2 done\n");
        index = 0;
        for(int p=0;p<var_cols;p++){
            boolean check_null = check_null_bitmap(null_bitmap,(p+fixed_cols));
            if(check_null){
                objects.add(null);
                index +=4;
            }
            else{
                int v_off = (record[index] & 0xFF) | ((record[index+1] & 0xFF )<< 8 );
                int v_len = (record[index+2] & 0xFF) | ((record[index+3] & 0xFF) << 8);
                index += 4;

                byte[] v_string;
                v_string = record_block.get_data(v_off,v_len);

                objects.add(new String(v_string));
            }
        }
//        System.out.print("Loop 3 done\n");
        return objects.toArray();
    }


    private List<RelDataType> convertInttoTypeList(List<Integer> type_list) {
        List<RelDataType> relTypeList = new ArrayList<>();
        RelDataTypeFactory type_factory = new JavaTypeFactoryImpl();

        for (int i=0; i<type_list.size(); i++) {
            int number = type_list.get(i);
            if (number == 0) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.VARCHAR));
            } else if (number == 1) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.INTEGER));
            } else if (number == 2) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.BOOLEAN));
            } else if (number == 3) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.FLOAT));
            } else if (number == 4) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.DOUBLE));
            } else {
                System.out.println("Invalid data type");
                throw new RuntimeException("Unsupported type");
            }
        }

        return relTypeList;

    }
    // the order of returned columns should be same as the order in schema
    // i.e., first all fixed length columns, then all variable length columns
    public List<Object[]> get_records_from_block(String table_name, int block_id){
        if (!check_file_exists(table_name) || block_id < 0)  return null;
        List<Object[]> parsed_records = new ArrayList<>();
        byte[] record_block = get_data_block(table_name, block_id);
        Block b = new Block(record_block);
        int offset = 2;
        int total_rows = (record_block[1] & 0xFF) | ((record_block[0] & 0xFF) << 8);
        int end = b.get_block_capacity() - 1;
        for (int i = 0; i < total_rows; i++) {
            int r_off = 0;
            r_off = (record_block[offset+1] & 0xFF) | ((record_block[offset] & 0xFF) << 8);
            offset+=2;
            byte[] record = b.get_data(r_off, end-r_off);
            Object[] o = convertToObjArr(record,table_name);
            parsed_records.add(o);
            end = r_off;
        }

        // TODO: Parse the data block and extract the records
        return parsed_records;
    }

    private int get_block_count(int file_id) {
        int i = 0;
        while (db.get_data(file_id, i) != null) {
            i++;
        }
        return i;
    }

    public boolean create_index(String table_name, String column_name, int order) {
        if(check_file_exists(table_name) == false) return false;
        if(check_index_exists(table_name,column_name)) return false;
        int file_id = file_to_fileid.get(table_name);
        int block_count = 0;
        while(db.get_data(file_id,block_count)!=null) block_count++;
        byte[] schema_byte = db.get_data(file_id,0);
        int total_cols = ( schema_byte[0] & 0xFF) | ((schema_byte[1] & 0xFF) <<8);
        int col_index = -1;
        RelDataType col_type = null;
        int fixed_offset = 0;
        int var_cols = 0;
        int fixed_cols = 0;
        boolean found = false;
        RelDataTypeFactory type_factory = new JavaTypeFactoryImpl();
        Block s_block = new Block(schema_byte);
        List<RelDataType> typeList = new ArrayList<>();
        List<String> names = new ArrayList<>();
        int j = 2;
        int end = s_block.get_block_capacity()-1;

//        List<Integer> typeList = new ArrayList<>();
        for(int i=0;i<total_cols;i++){
            int col_offset = ((schema_byte[j+1] & 0xFF) <<8) | (schema_byte[j] & 0xFF);
            j+=2;
            byte[] col = s_block.get_data(col_offset,end-col_offset);
            int datatype = col[0]& 0xFF;
            int len = col[1]& 0xFF;
            byte[] name = s_block.get_data(col_offset+2,len);
            String s_name = new String(name);
            names.add(new String(name));
            if(s_name.equals(column_name)){
                col_index = i;
                found = true;
            }
            switch(datatype){
                case 0 : var_cols += 1;
                    if(s_name.equals(column_name))  col_type = type_factory.createSqlType(SqlTypeName.VARCHAR);
                    typeList.add(type_factory.createSqlType(SqlTypeName.VARCHAR));
                    break;
                case 1 : fixed_cols += 1;
                    if(s_name.equals(column_name))  col_type = type_factory.createSqlType(SqlTypeName.INTEGER);
                    else if(!found) fixed_offset+=4;
                    typeList.add(type_factory.createSqlType(SqlTypeName.INTEGER));
                    break;
                case 2 : fixed_cols += 1;
                    if(s_name.equals(column_name)) col_type = type_factory.createSqlType(SqlTypeName.BOOLEAN);
                    else if(!found) fixed_offset+=1;
                    typeList.add(type_factory.createSqlType(SqlTypeName.BOOLEAN));
                    break;
                case 3 : fixed_cols += 1;
                    if(s_name.equals(column_name)) col_type = type_factory.createSqlType(SqlTypeName.FLOAT);
                    else if(!found) fixed_offset+=4;
                    typeList.add(type_factory.createSqlType(SqlTypeName.FLOAT));
                    break;
                case 4 : fixed_cols += 1;
                    if(s_name.equals(column_name)) col_type = type_factory.createSqlType(SqlTypeName.DOUBLE);
                    else if(!found) fixed_offset+=8;
                    typeList.add(type_factory.createSqlType((SqlTypeName.DOUBLE)));
                    break;
            }
            end = col_offset;
        }
//        System.out.print("Done with parsing cols \n");
        if(col_type == type_factory.createSqlType(SqlTypeName.VARCHAR)){
            BPlusTreeIndexFile<String> ifile = new BPlusTreeIndexFile<>(order,String.class);
            for(int k=1; k<block_count; k++){
                byte[] b_data = get_data_block(table_name,k);
                List<Object[]> r = get_records_from_block(table_name,k);
                for(Object[] p: r){
                    byte[] p_byte = convertToByteArray(p,typeList);
                    Block p_block = new Block(p_byte);
                    col_index = col_index-fixed_cols;
                    int off = 4*col_index;
                    byte[] col_off = p_block.get_data(off,2);
                    byte[] col_len = p_block.get_data(off+2,2);
                    int i_off = (col_off[0] & 0xFF) | ((col_off[1] & 0xFF) << 8);
                    int i_len = (col_len[0] & 0xFF) | ((col_len[1] & 0xFF) << 8);
                    byte[] col_val = p_block.get_data(i_off,i_len);
                    String s_val = new String(col_val);
                    ifile.insert(s_val,k);
                }
            }
            String index_file_name = table_name + "_" + column_name + "_index";
            int counter = db.addFile(ifile);
            file_to_fileid.put(index_file_name, counter);
        }
        else if(col_type == type_factory.createSqlType(SqlTypeName.INTEGER)){
            BPlusTreeIndexFile<Integer> ifile = new BPlusTreeIndexFile<>(order,Integer.class);
            for(int k=1; k<block_count; k++){
                byte[] b_data = get_data_block(table_name,k);
                List<Object[]> r = get_records_from_block(table_name,k);
                for(Object[] p: r){
                    byte[] p_byte = convertToByteArray(p,typeList);
                    Block p_block = new Block(p_byte);
                    int off = 4*var_cols + fixed_offset;
                    byte[] i_byte = p_block.get_data(off,4);
                    int i_val = (i_byte[0] & 0xFF) | ((i_byte[1] & 0xFF) << 8) | ((i_byte[2] & 0xFF) << 16) | ((i_byte[3] & 0xFF) << 24);
                    ifile.insert(i_val,k);
                }
            }
            String index_file_name = table_name + "_" + column_name + "_index";
            int counter = db.addFile(ifile);
            file_to_fileid.put(index_file_name, counter);
        }
        else if(col_type == type_factory.createSqlType(SqlTypeName.BOOLEAN)){
            BPlusTreeIndexFile<Boolean> ifile = new BPlusTreeIndexFile<>(order,Boolean.class);
            for(int k=1; k<block_count; k++){
                byte[] b_data = get_data_block(table_name,k);
                List<Object[]> r = get_records_from_block(table_name,k);
                for(Object[] p: r){
                    byte[] p_byte = convertToByteArray(p,typeList);
                    Block p_block = new Block(p_byte);
                    int off = 4*var_cols + fixed_offset;
                    byte[] i_byte = p_block.get_data(off,4);
                    boolean b_val = i_byte[0]!=0;
                    ifile.insert(b_val,k);
                }
            }
            String index_file_name = table_name + "_" + column_name + "_index";
            int counter = db.addFile(ifile);
            file_to_fileid.put(index_file_name, counter);
        }
        else if(col_type == type_factory.createSqlType(SqlTypeName.FLOAT)){
            BPlusTreeIndexFile<Float> ifile = new BPlusTreeIndexFile<>(order,Float.class);
            for(int k=1; k<block_count; k++){
                byte[] b_data = get_data_block(table_name,k);
                List<Object[]> r = get_records_from_block(table_name,k);
                for(Object[] p: r){
                    byte[] p_byte = convertToByteArray(p,typeList);
                    Block p_block = new Block(p_byte);
                    int off = 4*var_cols + fixed_offset;
                    byte[] i_byte = p_block.get_data(off,4);
                    int i_val = (i_byte[0] & 0xFF) | ((i_byte[1] & 0xFF) << 8) | ((i_byte[2] & 0xFF) << 16) | ((i_byte[3] & 0xFF) << 24);
                    float f_val = Float.intBitsToFloat(i_val);
                    ifile.insert(f_val,k);
                }
            }
            String index_file_name = table_name + "_" + column_name + "_index";
            int counter = db.addFile(ifile);
            file_to_fileid.put(index_file_name, counter);
        }
        else if(col_type == type_factory.createSqlType(SqlTypeName.DOUBLE)){
            BPlusTreeIndexFile<Double> ifile = new BPlusTreeIndexFile<>(order,Double.class);
            for(int k=1; k<block_count; k++){
                byte[] b_data = get_data_block(table_name,k);
                List<Object[]> r = get_records_from_block(table_name,k);
                for(Object[] p: r){
                    byte[] p_byte = convertToByteArray(p,typeList);
                    Block p_block = new Block(p_byte);
                    int off = 4*var_cols + fixed_offset;
                    byte[] b = p_block.get_data(off,4);
                    long i_val = (b[0] & 0xFF) | ((b[1] & 0xFF) << 8) | ((b[2] & 0xFF) << 16) | ((b[3] & 0xFF) << 24) | ((b[4] & 0xFF) << 32) | ((b[5] & 0xFF) << 40) | ((b[6] & 0xFF) << 48) | ((b[7] & 0xFF) << 56);
                    double f_val = Double.longBitsToDouble(i_val);
                    ifile.insert(f_val,k);
                }
            }
            String index_file_name = table_name + "_" + column_name + "_index";
            int counter = db.addFile(ifile);
            file_to_fileid.put(index_file_name, counter);
        }
        return true;
    }


    // returns the block_id of the leaf node where the key is present
    public int search(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
        byte[] val = (byte[]) value.getValue2();
        return db.search_index(file_id, val);
    }

    public boolean delete(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to delete from both - the file and the index
        int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
        byte[] val = (byte[]) value.getValue2();
        return db.delete_from_index(file_id, val);
    }

    // will be used for evaluation - DO NOT modify
    public DB getDb() {
        return db;
    }

    public <T> ArrayList<T> return_bfs_index(String table_name, String column_name) {
        if(check_index_exists(table_name, column_name)) {
            int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
            return db.return_bfs_index(file_id);
        } else {
            System.out.println("Index does not exist");
        }
        return null;
    }

}