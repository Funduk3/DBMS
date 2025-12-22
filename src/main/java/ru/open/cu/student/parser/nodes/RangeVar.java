package ru.open.cu.student.parser.nodes;

public class RangeVar {
    private final String catalog;
    private final String relname;
    private final String alias;

    public RangeVar(String catalog, String relname, String alias) {
        this.catalog = catalog;
        this.relname = relname;
        this.alias = alias;
    }

    public String getCatalog() { return catalog; }
    public String getRelname() { return relname; }
    public String getAlias() { return alias; }

    @Override
    public String toString() {
        return alias != null ? relname + " AS " + alias : relname;
    }
}