package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.util.List;


public class PFilter extends Filter implements PRel {

    public PFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RexNode condition) {
        super(cluster, traits, child, condition);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public String toString() {
        return "PFilter";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PFilter");
        /* Write your code here */
        PRel child = (PRel) getInput();
//        System.out.println("Child of filter is "+child);
        boolean f =child.open();
        return f;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PFilter");
        /* Write your code here */

        return;
    }


    private Object[] cached_row;
    private int flag = 0;
    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PFilter has next");
        /* Write your code here */
        PRel child = (PRel) getInput();
        boolean ans = false;
        while(child.hasNext() && ans==false){
            Object[] r = child.next();
            RexCall filter = (RexCall) getCondition();
            SqlOperator oper = filter.getOperator();
            List<RexNode> op = filter.getOperands();
            ans = compute(oper,op,r);
            if(ans==false) continue;
            cached_row = r;
            flag = 1;
            return true;
        }
        return false;
    }


    private BigDecimal convert_to_BigDecimal(Object val){
        if(val instanceof Integer || val instanceof Double || val instanceof Float) return new BigDecimal(val.toString());
        else if(val instanceof BigDecimal) return (BigDecimal) val;
        throw new IllegalArgumentException("Unsupported operand type: " + val.getClass());
    }

    private Object compute_operand(RexNode op, Object[] r){
        if(op instanceof RexInputRef){
            int i = ((RexInputRef) op).getIndex();
            if(i>=0 && i<r.length) return r[i];
            throw new IllegalArgumentException("Invalid column index: " + i);
        }
        else if(op instanceof RexCall){
            SqlOperator opr = ((RexCall) op).getOperator();
            List<RexNode> ops = ((RexCall) op).getOperands();
            BigDecimal ld = convert_to_BigDecimal(compute_operand(ops.get(0),r));
            BigDecimal rd = convert_to_BigDecimal(compute_operand(ops.get(1),r));
            switch (opr.getKind()){
                case PLUS: return ld.add(rd);
                case MINUS: return ld.subtract(rd);
                case TIMES: return ld.multiply(rd);
                case DIVIDE: return ld.divide(rd);
                default:  throw new IllegalArgumentException("Unsupported operator: " + opr.getKind());
            }
        }
        else if(op instanceof RexLiteral) return ((RexLiteral) op).getValue();
        return null;
    }

    private int compare(Object lval, Object rval){
        if(lval instanceof Integer || lval instanceof Double || lval instanceof Float || lval instanceof BigDecimal){
            BigDecimal ld = convert_to_BigDecimal(lval);
            return ((Comparable) ld).compareTo(rval);
        }
        else if(lval instanceof String){
            return ((Comparable) lval).compareTo(((NlsString)rval).getValue());
        }
        else if(lval instanceof Boolean){
            return (Boolean.compare((Boolean) lval, (Boolean) rval));
        }
        return -1;
    }

    private boolean compute(SqlOperator oper, List<RexNode> op, Object[]r){
        Object lval, rval;
        switch (oper.getKind()){
            case AND:
                return compute(((RexCall) op.get(0)).getOperator(), ((RexCall) op.get(0)).getOperands(), r) &&
                        compute(((RexCall) op.get(1)).getOperator(), ((RexCall) op.get(1)).getOperands(), r);
            case OR:
                return compute(((RexCall) op.get(0)).getOperator(), ((RexCall) op.get(0)).getOperands(), r) ||
                        compute(((RexCall) op.get(1)).getOperator(), ((RexCall) op.get(1)).getOperands(), r);
            case NOT:
                return !compute(((RexCall) op.get(0)).getOperator(), ((RexCall) op.get(0)).getOperands(), r);
            case EQUALS:
                lval = compute_operand(op.get(0), r);
                rval = compute_operand(op.get(1), r);
                return compare(lval, rval) == 0;
            case GREATER_THAN:
                lval = compute_operand(op.get(0), r);
                rval = compute_operand(op.get(1), r);
                return compare(lval, rval) > 0;
            case LESS_THAN:
                lval = compute_operand(op.get(0), r);
                rval = compute_operand(op.get(1), r);
                return compare(lval, rval) < 0;
            case GREATER_THAN_OR_EQUAL:
                lval = compute_operand(op.get(0), r);
                rval = compute_operand(op.get(1), r);
                return compare(lval, rval) >= 0;
            case LESS_THAN_OR_EQUAL:
                lval = compute_operand(op.get(0), r);
                rval = compute_operand(op.get(1), r);
                return compare(lval, rval) <= 0;
            default: throw new UnsupportedOperationException("Unsupported operator: " + oper.getKind());
        }
    }
    // returns the next row
    // Hint: Try looking at different possible filter conditions
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PFilter");
        /* Write your code here */
        Object[] r;
        if(flag == 1){ r = cached_row; flag=0;}
        else{ r = ((PRel)getInput()).next(); }

//        RexCall filter = (RexCall) getCondition();
//        SqlOperator oper = filter.getOperator();
//        List<RexNode> op = filter.getOperands();
//        boolean ans = compute(oper,op,r);
//        if(ans) return r;
        return r;
    }
}