public class SQLTermCollection {
    SQLTerm[] terms;
    Index index;
    String operator;
    public SQLTermCollection(SQLTerm[] terms, Index index, String operator) {
        this.terms = terms;
        this.index = index;
        this.operator = operator;
    }
    
}
