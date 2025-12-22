package ru.open.cu.student.parser.nodes;

public class AConst implements Expr {
    public Object value;

    public AConst(Object val) {
        this.value = val;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    public Object getVal() {
        return value;
    }
}
