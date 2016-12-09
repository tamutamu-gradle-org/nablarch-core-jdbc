package nablarch.core.db.cache;

import java.util.Map;

/**
 * データベースのテーブルに関するメタ情報を保持するクラス。
 * @author ryo asato
 */
public class TableDescriptor {

    private String tableName;

    private boolean isCaseSensitive;

    private Map<String, ColumnDescriptor> columnDescMap;

    public TableDescriptor(String tableName, boolean isCaseSensitive, Map<String, ColumnDescriptor> columnDescMap) {
        this.tableName = tableName;
        this.isCaseSensitive = isCaseSensitive;
        this.columnDescMap = columnDescMap;
    }

    /**
     * 指定したカラムの{@link ColumnDescriptor}を取得する。
     * 指定したカラムが存在しない場合、{@link IllegalArgumentException}を返す。
     * @param columnName カラム名
     * @return {@link ColumnDescriptor}
     */
    public ColumnDescriptor getColumnDescriptor(String columnName) {
        String key = isCaseSensitive ? columnName : columnName.toUpperCase();
        ColumnDescriptor columnDescriptor = columnDescMap.get(key);
        if (columnDescriptor == null) {
            throw new IllegalArgumentException("column not found. column: " + columnName);
        }
        return columnDescriptor;
    }

    /**
     * テーブル名を返す。
     * @return テーブル名
     */
    public String getTableName() {
        return tableName;
    }
}
