package ru.open.cu.student.ast;

public class AExpr extends Expr {
    public String op;          // оператор: "=", ">", "+", etc.
    public Expr left;          // левый операнд
    public Expr right;         // правый операнд

    public AExpr(String operator, Expr leftExpr, Expr rightExpr) {
        this.op = operator;
        this.left = leftExpr;
        this.right = rightExpr;
    }

    @Override
    public String toString() {
        return left + " " + op + " " + right;
    }

    public String getOp() {
        return op;
    }

    public Expr getLeft() {
        return left;
    }

    public Expr getRight() {
        return right;
    }
}