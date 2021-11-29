package tbc;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;

public class TableSchema implements Table {
    public String namespace;
    public String table;
    public ArrayList<Column> cols;
    public ArrayList<String>  primary_key;
    public ArrayList<String>  clustering_key;

    public String schema;
    public String stmt;

    public TableSchema() {
    }

    public TableSchema(String namespace, String table, ArrayList<Column> cols, ArrayList<String> primary_key, ArrayList<String> clustering_key) {
        this.namespace = namespace;
        this.table = table;
        this.cols = cols;
        this.primary_key = primary_key;
        this.clustering_key = clustering_key;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public ArrayList<Column> getCols() {
        return cols;
    }

    public void setCols(ArrayList<Column> cols) {
        this.cols = cols;
    }

    public ArrayList<String> getPrimary_key() {
        return primary_key;
    }

    public void setPrimary_key(ArrayList<String> primary_key) {
        this.primary_key = primary_key;
    }

    public ArrayList<String> getClustering_key() {
        return clustering_key;
    }

    public void setClustering_key(ArrayList<String> clustering_key) {
        this.clustering_key = clustering_key;
    }

    @Override
    public Object[] convert(String[] data) throws ParseException {
        ArrayList<Object> alo = new ArrayList<>();

        for(int i=0; i<data.length; i++) {
            alo.add(TypeMapper.map(cols.get(i).type, data[i]));
        }
        return alo.toArray();
    }

    @Override
    public String getTableName() {
        return this.table;
    }

    @Override
    public String getSchema() {
        if (this.schema != null && this.schema.length() > 0)
            return this.schema;

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("CREATE TABLE %s.%s (\n", namespace, table));
        for(Column col: cols) {
            sb.append(String.format("    %s %s,\n", col.col_name, col.type));
        }
        String pk;
        if (primary_key.size() == 1){
            pk = primary_key.get(0);
        } else {
            pk = String.format("(%s)", String.join(",", primary_key));
        }

        if (clustering_key == null || clustering_key.size() == 0)
            sb.append(String.format("PRIMARY KEY (%s))", pk));
        else
            sb.append(String.format("PRIMARY KEY (%s, %s))", pk, String.join(", ", clustering_key)));

        this.schema = sb.toString();
        return this.schema;
    }

    @Override
    public String getStmt() {
        if (this.stmt != null && this.stmt.length() > 0)
            return this.stmt;

        ArrayList<String> col_names = new ArrayList<>();

        for(Column cols: cols) {
            col_names.add(cols.col_name);
        }
        String stmt = String.format("INSERT INTO %s.%s(%s) VALUES (%s)",
                namespace,
                table,
                String.join(", ", col_names),
                String.join(", ", Collections.nCopies(col_names.size(), "?"))
        );

        this.stmt = stmt;
        return stmt;
    }
}
