package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import convention.PConvention;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

// Count, Min, Max, Sum, Avg
public class PAggregate extends Aggregate implements PRel {

    public PAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new PAggregate(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public String toString() {
        return "PAggregate";
    }


    private List<Object[]> all_rows = new ArrayList<>();
    private List<Object[]> ans_rows = new ArrayList<>();
    private int idx = 0;
    private int len = 0;
    private int ans_size =0;
    private int row_len = 0;
    private int flag = 1;
    HashMap<List<Object>,Integer> dhm = new HashMap<>();
    HashMap<Object,Integer> shm = new HashMap<>();
    HashMap<Object,Integer> ahm = new HashMap<>();
    List<Object> dlist = new ArrayList<>();
    List<HashMap<List<Object>,Integer>> hash_list = new ArrayList<>();
    private void get_all_rows(PRel node) {
        while (node.hasNext()) {
            Object[] row = node.next();
            if (row != null) all_rows.add(row);
        }
        len = all_rows.size();
    }


    private boolean check_for_null(Object[] r, List<Integer> argList, boolean distinct){
        boolean b = true;
        dlist = new ArrayList<>();
        for(int l = 0; l < argList.size(); l++){
            if(r[argList.get(l)]==null){ b = false; break;}
            else if(distinct) dlist.add(r[argList.get(l)]);
        }
        return b;
    }
    // returns true if successfully opened, false otherwise

    private Object compute_sum(Object[] r, int p, SqlTypeName type, Object cnt_i, Object cnt_f, Object cnt_d, Object ans_val, boolean flag){
        if (type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT) {if(flag){  cnt_i = ((Integer) cnt_i) + (Integer) r[p];} ans_val = cnt_i;}
        else if (type == SqlTypeName.FLOAT && flag) {if(flag){ cnt_f = (Float) cnt_f + (Float) r[p];} ans_val = cnt_f;}
        else if (type == SqlTypeName.DOUBLE && flag) {if(flag){cnt_d = (Double) cnt_d + (Double) r[p];} ans_val = cnt_d;}
        return ans_val;
//        if (type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT) {cnt_i = ((Integer) cnt_i) + (Integer) r[p]; ans_val = cnt_i;}
//        else if (type == SqlTypeName.FLOAT) {cnt_f = (Float) cnt_f + (Float) r[p]; ans_val = cnt_f;}
//        else if (type == SqlTypeName.DOUBLE) {cnt_d = (Double) cnt_d + (Double) r[p]; ans_val = cnt_d;}
//        break;
//        if((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT)){ if(hml.size()==j) hml.add(0); cnt = hml.get(j); if(r[p]!=null) cnt = (Integer) cnt + (Integer) r[p];}
//        else if(type == SqlTypeName.FLOAT){ if(hml.size()==j) hml.add(0); cnt = hml.get(j); if(r[p]!=null) cnt = (Float) cnt + (Float) r[p];}
//        else if (type == SqlTypeName.DOUBLE){ if(hml.size()==j) hml.add(0); cnt = hml.get(j); if(r[p]!=null) cnt = (Double) cnt + (Double) r[p];}
//        break;

//        if((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT)) {Object cnt =0; if( r[p] != null) {cnt = (Integer) cnt + (Integer) r[p]; new_l.add(cnt);} else new_l.add(cnt);}
//        else if(type == SqlTypeName.FLOAT) {Object cnt =0.0f; if( r[p] != null) {cnt = (Float) cnt + (Float) r[p]; new_l.add(cnt);} else new_l.add(cnt);}
//        else if(type == SqlTypeName.DOUBLE) {Object cnt =0.0; if( r[p] != null) {cnt = (Double) cnt + (Double) r[p]; new_l.add(cnt);} else new_l.add(cnt);}
//
    }

    private Object compute_sum_distinct(Object[] r, int p, SqlTypeName type, Object cnt_i, Object cnt_f, Object cnt_d, Object ans_val, boolean flag, boolean distinct){
        if (type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT) {if(flag && distinct && !shm.containsKey(r[p])){ shm.put(r[p],1); cnt_i = ((Integer) cnt_i) + (Integer) r[p];} ans_val = cnt_i;}
        else if (type == SqlTypeName.FLOAT && flag) {if(flag && distinct && !shm.containsKey(r[p])){ shm.put(r[p],1); cnt_f = (Float) cnt_f + (Float) r[p];} ans_val = cnt_f;}
        else if (type == SqlTypeName.DOUBLE && flag) {if(flag && distinct && !shm.containsKey(r[p])) {shm.put(r[p],1);cnt_d = (Double) cnt_d + (Double) r[p];} ans_val = cnt_d;}
        return ans_val;
    }

    private Object compute_max(Object[] r, int p, SqlTypeName type, Object max_i, Object max_f, Object max_d, Object ans_val){
        if ((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT)){if(r[p]!=null && ((Integer) max_i).compareTo((Integer) r[p]) < 0) max_i = r[p]; ans_val = max_i;}
        else if ((type == SqlTypeName.FLOAT)){if(r[p]!=null && ((Float) max_f).compareTo((Float) r[p]) < 0) max_f = r[p]; ans_val = max_f;}
        else if ( (type == SqlTypeName.DOUBLE)){if(r[p]!=null && ((Double) max_d).compareTo((Double) r[p]) < 0) max_d = r[p]; ans_val = max_d;}
        return ans_val;
    }

    private Object compute_min(Object[] r, int p, SqlTypeName type, Object min_i, Object min_f, Object min_d, Object ans_val){
        if ((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT)){if(r[p]!=null && ((Integer) min_i).compareTo((Integer) r[p]) > 0) min_i = r[p]; ans_val = min_i;}
        else if ((type == SqlTypeName.FLOAT)){if(r[p]!=null && ((Float) min_f).compareTo((Float) r[p]) > 0) min_f = r[p]; ans_val = min_f;}
        else if ((type == SqlTypeName.DOUBLE)){if(r[p]!=null && ((Double) min_d).compareTo((Double) r[p]) > 0) min_d = r[p]; ans_val = min_d;}
        return ans_val;
    }

    private Object compute_avg(Object[] r,int p, SqlTypeName type, Object cnt_i, Object cnt_f, Object cnt_d, int count, Object ans_val){
        if ((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT) && r[p]!=null ) {cnt_i = ((Integer) cnt_i) + (Integer) r[p]; count++; if(count>0 )ans_val = (Integer)cnt_i/count;}
        else if ((type == SqlTypeName.FLOAT) && r[p]!=null) {cnt_f = (Float) cnt_f + (Float) r[p]; count++; if(count>0) ans_val = (Float)cnt_f/count;}
        else if ((type == SqlTypeName.DOUBLE) && r[p]!=null) {cnt_d = (Double) cnt_d + (Double) r[p]; count++; if(count>0) ans_val = (Double)cnt_d/count;}
        return ans_val;
    }

    private Object compute_avg_distinct(Object[] r,int p, SqlTypeName type, Object cnt_i, Object cnt_f, Object cnt_d, int count, Object ans_val){
        if ((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT)){ if(r[p]!=null && !ahm.containsKey(r[p])) {cnt_i = ((Integer) cnt_i)*count + (Integer) r[p]; count++;} if(count>0 )ans_val = (Integer)cnt_i/count;}
        else if ((type == SqlTypeName.FLOAT)){ if(r[p]!=null && !ahm.containsKey(r[p])) {cnt_f = ((Float) cnt_f)*count + (Float) r[p]; count++;} if(count>0 )ans_val = (Float)cnt_f/count;}
        else if ((type == SqlTypeName.DOUBLE)){ if(r[p]!=null && !ahm.containsKey(r[p])) {cnt_d = ((Integer) cnt_d)*count + (Integer) r[p]; count++;} if(count>0 )ans_val = (Integer)cnt_d/count;}
        return ans_val;
    }
    @Override
    public boolean open() {
        logger.trace("Opening PAggregate");
        /* Write your code here */
        List<RelDataTypeField> rel_fields = getRowType().getFieldList();
        List<SqlTypeName> typelist = new ArrayList<>();
        for(RelDataTypeField rf : rel_fields) typelist.add(rf.getType().getSqlTypeName());
        PRel child = (PRel) getInput();
//        System.out.println("Child is "+child);
        boolean f =child.open();
        if(!f) return false;
        get_all_rows(child);
        row_len = typelist.size();


        if(groupSet.isEmpty()){
            Object[] ans_row = new Object[row_len];
            for(int i=0; i<aggCalls.size(); i++){
                SqlTypeName type = typelist.get(i);
                dhm = new HashMap<>();
                if(aggCalls.get(i).getArgList().size()!=0){
                    int p = aggCalls.get(i).getArgList().get(0);
                    Object cnt_i = 0, cnt_f = 0.0f , cnt_d = 0.0;
                    Object max_i = Integer.MIN_VALUE, max_f = Float.MIN_VALUE, max_d = Double.MIN_VALUE;
                    Object min_i = Integer.MAX_VALUE, min_f = Float.MAX_VALUE, min_d = Double.MAX_VALUE;
                    Object ans_val = 0;
                    int count = 0;
                    for(int j=0 ;j<len; j++){
                        Object[] r = all_rows.get(j);
                        if(r[p]!=null){
                            switch ((aggCalls.get(i)).getAggregation().getName()) {
                                case "COUNT": if (type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT){
                                    if(aggCalls.get(i).isDistinct()){
                                        if(check_for_null(r,aggCalls.get(i).getArgList(),true)){
                                            if(!dhm.containsKey(dlist)){dhm.put(dlist,1);ans_val = ((Integer) ans_val) + 1; }
                                        }
                                    }
                                    else if(check_for_null(r,aggCalls.get(i).getArgList(), false)) ans_val = ((Integer) ans_val) + 1;
                                    break;}
                                case "SUM":
                                    if(aggCalls.get(i).isDistinct()) ans_val = compute_sum_distinct(r,p,type,cnt_i,cnt_f,cnt_d,ans_val,true,true);
                                    else ans_val = compute_sum(r,p,type,cnt_i,cnt_f,cnt_d,ans_val,true); break;
                                case "MAX": ans_val = compute_max(r,p,type,max_i,max_f,max_d,ans_val); break;
                                case "MIN": ans_val = compute_min(r,p,type,min_i,min_f,min_d,ans_val); break;
                                case "AVG":
                                    if(aggCalls.get(i).isDistinct()) ans_val = compute_avg_distinct(r,p,type,cnt_i,cnt_f,cnt_d,count,ans_val);
                                    else ans_val = compute_avg(r,p,type,cnt_i,cnt_f,cnt_d,count,ans_val);
//                                    if ((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT) && r[p]!=null ) {cnt_i = ((Integer) cnt_i) + (Integer) r[p]; count++; if(count>0 )ans_val = (Integer)cnt_i/count;}
//                                    else if ((type == SqlTypeName.FLOAT) && r[p]!=null) {cnt_f = (Float) cnt_f + (Float) r[p]; count++; if(count>0) ans_val = (Float)cnt_f/count;}
//                                    else if ((type == SqlTypeName.DOUBLE) && r[p]!=null) {cnt_d = (Double) cnt_d + (Double) r[p]; count++; if(count>0) ans_val = (Double)cnt_d/count;}
//                                    break;
                            }
                        }
                    }
                    ans_row[i] = ans_val;
                }
                else if( (aggCalls.get(i)).getAggregation().getName() == "COUNT"){
                    ans_row[i] = len;
                }
            }
            ans_rows.add(ans_row);
        }
        else{
//            System.out.println("non empty groupset all_rows size is "+all_rows.size());
            if(all_rows.size()!=0){
                HashMap<List<Object>, List<Object>> hm = new HashMap<>();
                int c = 0;
//                int count  =0;
                HashMap<List<Object>,Integer> count_hm = new HashMap<>();
                for(int kk=0;kk<aggCalls.size();kk++){
                    HashMap<List<Object>,Integer>hp = new HashMap<>();
                    hash_list.add(hp);
                }
                for(int u = 0; u<len; u++){
                    Object[] r = all_rows.get(u);

                    List<Object> list_r = new ArrayList<>();
                    for (Integer columnIndex : groupSet) {
                        list_r.add(r[columnIndex]);
//                        System.out.print(r[columnIndex]+" ");
                    }
                    if(aggCalls.isEmpty() && !hm.containsKey(list_r)){
                        List<Object> q = new ArrayList<>();
                        q.add(1);
                        hm.put(list_r,q);
                    }
                    for( int j = 0; j<aggCalls.size(); j++){
//                        System.out.println("start iter");
                        HashMap<List<Object>,Integer> ghm = hash_list.get(j);
                        int g = 0;
                        for (Integer value : groupSet) g++;

//                        int begin = all_rows.get(0).length + j;
                        int begin = g + j;
//                        System.out.println("Size of typelist "+typelist.size()+"begin is "+begin);
                        SqlTypeName type = typelist.get(begin);
//                        System.out.println("type is "+type);
                        if(aggCalls.get(j).getArgList().size()!=0){
                            int p = aggCalls.get(j).getArgList().get(0);
//                            System.out.println("r len is "+ r.length+" row_len is "+ row_len+" p is "+p);
                            if(hm.containsKey(list_r)){
                                List<Object> hml = hm.get(list_r);

                                Object cnt = null;
                                switch ((aggCalls.get(j)).getAggregation().getName()) {
                                    case "COUNT": /*cnt = hml.get(j); if(r[p]!=null) cnt = (Integer)cnt +1; break;*/
                                    if((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT)){
                                        if(hml.size() == j) hml.add(0); cnt = hml.get(j);
                                        if(aggCalls.get(j).isDistinct()){
                                            if(check_for_null(r,aggCalls.get(j).getArgList(),true)){
                                                if(!ghm.containsKey(dlist)){ghm.put(dlist,1);cnt = ((Integer) cnt) + 1; }
                                            }
                                        }
                                        else if(check_for_null(r, aggCalls.get(j).getArgList(),false)) cnt = ((Integer) cnt) + 1; break;
                                    }
                                    case "SUM" :
                                        if((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT)){ if(hml.size()==j) hml.add(0);}
                                        else if(type == SqlTypeName.FLOAT){ if(hml.size()==j) hml.add(0.0f);}
                                        else if (type == SqlTypeName.DOUBLE){ if(hml.size()==j) hml.add(0.0);}
                                        Object inter_cnt = hml.get(j);
                                        if(!aggCalls.get(j).isDistinct()) cnt = compute_sum(r,p,type,inter_cnt,inter_cnt,inter_cnt,0, (r[p]!=null));
                                        else{
                                            if(r[p]!=null){
                                                List<Object> inter = new ArrayList<>(); inter.add(r[p]);
                                                if(!ghm.containsKey(inter)){
                                                    ghm.put(inter,1);
//                                                    System.out.println("evaluating sum when key");
                                                    if((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT)){  inter_cnt = (Integer) inter_cnt + (Integer) r[p];}
                                                    else if(type == SqlTypeName.FLOAT){ inter_cnt = (Float) inter_cnt + (Float) r[p];}
                                                    else if (type == SqlTypeName.DOUBLE){ inter_cnt = (Double) inter_cnt + (Double) r[p];}
                                                    cnt = inter_cnt;
            //                                        System.out.println("done evaluating sum when key");
                                                }
                                            }
                                        }
                                        break;
                                    case "MAX" :
                                        if((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT)){ if(hml.size()==j) hml.add(Integer.MIN_VALUE);}
                                        else if(type == SqlTypeName.FLOAT){ if(hml.size()==j) hml.add(Float.MIN_VALUE);}
                                        else if (type == SqlTypeName.DOUBLE){ if(hml.size()==j) hml.add(Double.MIN_VALUE);}
                                        Object inter_cnt2 = hml.get(j);
                                        cnt = compute_max(r,p,type,inter_cnt2,inter_cnt2,inter_cnt2,0);
                                        break;
                                    case "MIN":
                                        if((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT)){ if(hml.size()==j) hml.add(Integer.MAX_VALUE);}
                                        else if(type == SqlTypeName.FLOAT){ if(hml.size()==j) hml.add(Float.MAX_VALUE);}
                                        else if (type == SqlTypeName.DOUBLE){ if(hml.size()==j) hml.add(Double.MAX_VALUE);}
                                        Object inter_cnt3 = hml.get(j);
                                        cnt = compute_min(r,p,type,inter_cnt3,inter_cnt3,inter_cnt3,0);
                                        break;
                                    case "AVG":int count = count_hm.get(list_r);
                                        if((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT)){ if(hml.size()==j) hml.add(0); cnt = hml.get(j); if(r[p]!=null){ cnt = ((Integer) cnt)*count + (Integer) r[p]; count++; cnt = (Integer)cnt/count;count_hm.put(list_r,count);}
//                                            if((Integer)(list_r.get(0)) == 134) System.out.println("Count is "+ count + " Cnt stored is "+ cnt+" r[p] is "+r[p]);
                                        }
                                        else if(type == SqlTypeName.FLOAT){ if(hml.size()==j) hml.add(0.0f); cnt = hml.get(j); if(r[p]!=null) { cnt = ((Float) cnt)*count + (Float) r[p]; count++; cnt = (Float)cnt/count;count_hm.put(list_r,count);}}
                                        else if (type == SqlTypeName.DOUBLE){

                                            if(hml.size()==j) hml.add(0.0); cnt = hml.get(j); if(r[p]!=null) { cnt = ((Double) cnt)*count + (Double) r[p]; count++; cnt = (Double)cnt/count; count_hm.put(list_r,count);}
                                            if((Integer)(list_r.get(0)) == 134) System.out.println("Count is "+ count + " Cnt stored is "+ cnt+" r[p] is "+r[p]);
                                        }
                                        break;

                                }
                                hml.set(j, cnt);
//                                System.out.println("j is "+j+" cnt is "+ (Integer)cnt);
                                hm.put(list_r,hml);
//                                System.out.println("exit from if");
                            }
                            else{
//                                System.out.println("In Else now should enter sum "+ (aggCalls.get(j)).getAggregation().getName());
//                                int count = 0;
                                List<Object> new_l = new ArrayList<>();
                                switch((aggCalls.get(j)).getAggregation().getName()){
                                    case "COUNT" : if((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT)){if(check_for_null(r,aggCalls.get(j).getArgList(),false)) new_l.add(1); else new_l.add(0); break;}
                                    case "SUM" : Object fval = compute_sum(r,p,type,0,0.0f,0.0,null,(r[p]!=null)); new_l.add(fval); break;
                                    case "MAX" : Object fval1 = compute_max(r,p,type,Integer.MIN_VALUE,Float.MIN_VALUE,Double.MIN_VALUE,null); new_l.add(fval1); break;
                                    case "MIN": Object fval2 = compute_min(r,p,type,Integer.MAX_VALUE,Float.MAX_VALUE,Double.MAX_VALUE,null); new_l.add(fval2);break;
                                    case "AVG":
                                        if((type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT)) {
                                            Object cnt =0; if( r[p] != null) {cnt = ((Integer) cnt) + (Integer) r[p];} new_l.add(cnt);
//                                            if((Integer)(list_r.get(0)) == 134) System.out.println("First Cnt stored is "+ cnt);
                                        }
                                        else if(type == SqlTypeName.FLOAT) {Object cnt =0.0f; if( r[p] != null) {cnt = ((Float) cnt) + (Float) r[p];}  new_l.add(cnt);}
                                        else if(type == SqlTypeName.DOUBLE) {Object cnt =0.0; if( r[p] != null) {cnt = (Double)cnt + (Double) r[p];} new_l.add(cnt);
//                                            if((Integer)(list_r.get(0)) == 134) System.out.println("First Cnt stored is "+ cnt);
                                        }
                                        break;
                                }

                                count_hm.put(list_r,1);
                                hm.put(list_r,new_l);
                                c++;
                            }
                        }
                        else{
                            if((aggCalls.get(j)).getAggregation().getName() == "COUNT") {
                                if (hm.containsKey(list_r) && (type ==SqlTypeName.INTEGER || type == SqlTypeName.BIGINT)){
                                    List<Object> hml = hm.get(list_r);
                                    Object cnt = hml.get(j);
                                    cnt = (Integer)cnt+1;
                                    hml.set(j,cnt);
                                    hm.put(list_r,hml);
                                }
                                else{
                                    List<Object> hml = new ArrayList<>();
                                    hml.add(1);
                                    hm.put(list_r,hml);
                                    c++;
                                }
                            }
                        }

                    }



                }
//                System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@");
//                for (List<Object> key: hm.keySet()) {
//                    Object[] row = new Object[row_len];
//                    for (int j=0; j<all_rows.get(0).length; j++) {
//                        row[j] = key.get(j);
//                    }
//                    List<Object> value = hm.get(key);
//                    for (int j=0; j<aggCalls.size(); j++) {
//                        row[j + all_rows.get(0).length] = value.get(j);
//                    }
////                    for(int i=0;i<row.length;i++){
////                        System.out.print(row[i]+" ");
////                    }
////                    System.out.println("");
//                    ans_rows.add(row);
//                }


                for (HashMap.Entry<List<Object>, List<Object>> entry : hm.entrySet()) {
                    List<Object> key = entry.getKey();
                    List<Object> value = entry.getValue();
                    Object[] row = new Object[key.size() + value.size()];
                    for (int j = 0; j < key.size(); j++) {
                        row[j] = key.get(j);
                    }
                    for (int j = 0; j < value.size(); j++) {
                        row[key.size() + j] = value.get(j);
                    }
                    ans_rows.add(row);
                }
            }
        }
        ans_size = ans_rows.size();
//        System.out.println("all rows size is "+ all_rows.size());
//        for (Object[] array
//        : ans_rows) {
//            for (Object element : array) {
//                System.out.print(element + " ");
//            }
//            System.out.println(); // Move to the next line after printing each array
//        }
        return true;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PAggregate");
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PAggregate has next");
        /* Write your code here */
        if (idx >= ans_size) {
            return false;
        }
        return true;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PAggregate");
        if (idx == ans_size) {
            return null;
        }
        Object[] row = ans_rows.get(idx);
        idx++;
        return row;
    }

}