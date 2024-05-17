package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.sql.SqlOperator;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/*
    * Implement Hash Join
    * The left child is blocking, the right child is streaming
*/
public class PJoin extends Join implements PRel {

    public PJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
                super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
                assert getConvention() instanceof PConvention;
    }

    @Override
    public PJoin copy(
            RelTraitSet relTraitSet,
            RexNode condition,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new PJoin(getCluster(), relTraitSet, left, right, condition, variablesSet, joinType);
    }

    @Override
    public String toString() {
        return "PJoin";
    }


    private List<Object[]> all_rows = new ArrayList<>();
    private int idx = 0;
    private int len = 0;

    private Object compute_operand(RexNode op, Object[] r, int l) {
        if (op instanceof RexInputRef) {
            int i = ((RexInputRef) op).getIndex();
            i = i -l;
            if (i >= 0 && i < r.length) return r[i];
            throw new IllegalArgumentException("Invalid column index: " + i);
        }
        return null;
    }

    private String compute(SqlOperator operator, List<RexNode> operands, Object[] row, int len, boolean flag ) {
        switch(operator.getKind()) {
            case AND:
                String a1=  compute(((RexCall) operands.get(0)).getOperator(), ((RexCall) operands.get(0)).getOperands(), row, len, flag);
                String a2 = compute(((RexCall) operands.get(1)).getOperator(), ((RexCall) operands.get(1)).getOperands(), row, len, flag);
                StringBuilder key = new StringBuilder();
                key.append(a1+"#"+a2);
                return key.toString();
            case EQUALS:
                Object lval3 = compute_operand(operands.get(0), row,0);
                Object rval3 = compute_operand(operands.get(1), row,len);
                if(flag==true) return lval3.toString();
                return rval3.toString();
        }
        return null;
    }


    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PJoin");
        /* Write your code here */
        PRel left_input = (PRel) getLeft();
        left_input.open();
        PRel right_input = (PRel) getRight();
        right_input.open();

        RexCall filter = (RexCall) getCondition();
        SqlOperator oper = filter.getOperator();
        List<RexNode> op = filter.getOperands();
        HashMap<String, ArrayList<Object[]>> left_hash = new HashMap<>();
        HashMap<String, ArrayList<Object[]>> right_hash = new HashMap<>();
        int left_length = 0;
        int right_length = 0;

        ArrayList<Object[]> left_rows =new ArrayList<>();
        ArrayList<Object[]> right_rows = new ArrayList<>();

        if(getJoinType()!=JoinRelType.LEFT)
        {
            while (left_input.hasNext()) {
                Object[] r = left_input.next();
                if(getJoinType()==JoinRelType.FULL) left_rows.add(r);
                left_length = r.length;
                String key = compute(oper, op, r, left_length, true);
                if (!left_hash.containsKey(key)) left_hash.put(key, new ArrayList<>());
                left_hash.get(key).add(r);
            }
//            System.out.println("lapse 1");
            while (right_input.hasNext()) {
//                System.out.println("lapse 3");
                Object[] r = right_input.next();
                if(getJoinType()==JoinRelType.FULL) right_rows.add(r);
//                System.out.println("lapse 4");
                String key = compute(oper, op, r, left_length, false);
//                System.out.println("lapse 5");
                boolean got_matched = false;
//                System.out.println("lapse 6");
                if (left_hash.containsKey(key)) {
                    ArrayList<Object[]> left_match = left_hash.get(key);
//                    System.out.println("lapse 7");
                    got_matched = true;
                    for (Object[] p : left_match) {
                        Object[] combined_row = new Object[p.length + r.length];
                        System.arraycopy(p, 0, combined_row, 0, p.length);
                        System.arraycopy(r, 0, combined_row, p.length, r.length);
                        all_rows.add(combined_row);
                    }
//                    System.out.println("lapse 8");
                }
//                System.out.println("lapse 9");
                if ((getJoinType() == JoinRelType.RIGHT) || (getJoinType() == JoinRelType.FULL)) {
//                    System.out.println("lapse 10");
                    if (!got_matched) {
                        Object[] combined_row = new Object[left_length + r.length];
                        for (int i = 0; i < left_length; i++) {
                            combined_row[i] = null;
                        }
                        System.arraycopy(r, 0, combined_row, left_length, r.length);
                        all_rows.add(combined_row);
                    }
                }
            }
//            System.out.println("lapse 2");
        }

        if(getJoinType()==JoinRelType.LEFT || getJoinType()==JoinRelType.FULL){
            Object[] rigrow = new Object[0];
            boolean flag = false;

            if(getJoinType()==JoinRelType.LEFT ) {
                if (left_input.hasNext()) {
                    rigrow = left_input.next();
                    left_length = rigrow.length;
                    flag = true;
                }
                while (right_input.hasNext()) {
                    Object[] r = right_input.next();
                    right_length = r.length;
                    String key = compute(oper, op, r, left_length, false);
                    if (!right_hash.containsKey(key)) right_hash.put(key, new ArrayList<>());
                    right_hash.get(key).add(r);
                }
            }
            else {
                for (Object[] r : right_rows) {
                    right_length = r.length;
                    String key = compute(oper, op, r, left_length, false);
                    if (!right_hash.containsKey(key)) right_hash.put(key, new ArrayList<>());
                    right_hash.get(key).add(r);
                }
            }
            int t = 0;
            while (flag == true || t < left_rows.size() || left_input.hasNext()) {
                Object[] r;
                if (flag) {
                    r = rigrow;
                    flag = false;
//                    t++;
                } else {
                    if (getJoinType() == JoinRelType.LEFT) r = left_input.next();
                    else {r = left_rows.get(t);
                    t++;}
                }
                String key = compute(oper, op, r, left_length, true);
                boolean got_matched = false;
                if( right_hash.containsKey(key)){
                    ArrayList<Object[]> right_match = right_hash.get(key);
                    got_matched = true;
                    if(getJoinType()==JoinRelType.LEFT) {
                        for (Object[] p : right_match) {
                            Object[] combined_row = new Object[p.length + r.length];
                            System.arraycopy(p, 0, combined_row, r.length, p.length);
                            System.arraycopy(r, 0, combined_row, 0, r.length);
                            all_rows.add(combined_row);
                        }
                    }
                }
                if(!got_matched){
                    Object[] combined_row = new Object[right_length + r.length];
                    for(int i=r.length;i<combined_row.length;i++){
                        combined_row[i]=null;
                    }
                    System.arraycopy(r,0,combined_row,0,r.length);
                    all_rows.add(combined_row);
                }
                if(t == left_rows.size() && getJoinType()==JoinRelType.FULL) break;
                boolean f2 = (t < left_rows.size());
            }
        }
        len = all_rows.size();
//        System.out.println("Result size is "+all_rows.size());
        return true;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PJoin");
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PJoin has next");
        /* Write your code here */
        if(idx >= len) return false;
        return true;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PJoin");
        /* Write your code here */
        if(idx >= len) return null;
        idx++;
        return all_rows.get(idx-1);
    }
}
