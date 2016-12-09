package nablarch.core.db.cache;

/**
 * データベースのカラムに関するメタ情報を保持するクラス。
 * @author ryo asato
 */
public class ColumnDescriptor {

    private String columnName;

    private int columnType;

    public ColumnDescriptor(String columnName, int columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    /**
     * カラム名を取得する。
     * @return カラム名
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * SQL型を取得する。
     * @return {@link java.sql.Types}からのSQL型
     */
    public int getColumnType() {
        return columnType;
    }

}