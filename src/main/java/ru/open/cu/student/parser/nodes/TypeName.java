package ru.open.cu.student.parser.nodes;

public class TypeName implements AstNode {
    public String name;
    public TypeName(String typeName) {
        this.name = typeName;
    }

    @Override
    public String toString() {
        return name;
    }

    public String getName() {
        return name;
    }
}
