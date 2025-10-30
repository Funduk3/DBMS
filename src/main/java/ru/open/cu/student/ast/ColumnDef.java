package ru.open.cu.student.ast;

public class ColumnDef implements AstNode {
    public String colname;     // имя столбца
    public TypeName typeName;  // тип

    public ColumnDef(String name, TypeName type) {
        this.colname = name;
        this.typeName = type;
    }

    @Override
    public String toString() {
        return typeName.toString() + " " + colname;
    }

    public String getColname() {
        return colname;
    }

    public TypeName getTypeName() {
        return typeName;
    }
}