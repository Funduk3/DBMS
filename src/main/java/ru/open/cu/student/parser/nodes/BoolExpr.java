package ru.open.cu.student.parser.nodes;

import java.util.List;

public class BoolExpr implements Expr {
    public String boolop;
    public List<Expr> args;

    public BoolExpr(String op, List<Expr> arguments) {
        this.boolop = op;
        this.args = arguments;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < args.size(); i++) {
            sb.append(args.get(i).toString());
            if (i < args.size() - 1) {
                sb.append(" ");
                sb.append(boolop);
            }
        }
        return sb.toString();
    }
}