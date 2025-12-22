package ru.open.cu.student.lexer;

public class Token {
    private final String type;
    private final String value;

    public Token(String type, String value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public String toString() {
        if (type.equals("IDENT") || type.equals("NUMBER")) {
            return type + "(" + value + ")";
        }
        return type;
    }

    public String getType() {
        return type;
    }

    public String getValue() {
        return value;
    }
}
