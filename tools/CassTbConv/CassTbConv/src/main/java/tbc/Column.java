package tbc;

public class Column {
    public String col_name;
    public String type;

    public Column() {
    }

    public Column(String col_name, String type) {
        this.col_name = col_name;
    }

    public String getCol_name() {
        return col_name;
    }

    public void setCol_name(String col_name) {
        this.col_name = col_name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
