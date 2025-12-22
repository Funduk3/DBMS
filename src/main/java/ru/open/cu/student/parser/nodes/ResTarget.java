package ru.open.cu.student.parser.nodes;

public class ResTarget {
    private final Expr val;
    private final String name;

    public ResTarget(Expr val, String name) {
        this.val = val;
        this.name = name;
    }

    public Expr getVal() { return val; }
    public String getName() { return name; }

    @Override
    public String toString() {
        return name != null ? val.toString() + " AS " + name : val.toString();
    }
}