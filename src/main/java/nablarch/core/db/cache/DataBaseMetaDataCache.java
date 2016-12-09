package nablarch.core.db.cache;

import nablarch.core.db.DbAccessException;
import nablarch.core.util.StringUtil;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * データベースのメタデータをキャッシュするクラス。
 * @author ryo asato
 */
public class DataBaseMetaDataCache {

    private static DataBaseMetaDataCache instance = null;
    private Map<String, TableDescriptor> tableDescriptorMap = new HashMap<String, TableDescriptor>();

    private DataBaseMetaDataCache() {
    }

    public static synchronized DataBaseMetaDataCache getInstance() {
        if (instance == null) {
            instance = new DataBaseMetaDataCache();
        }
        return instance;
    }

    /**
     * 指定したテーブルに対応する{@link TableDescriptor}を取得する。
     * スキーマ名を指定しなかった場合、デフォルトスキーマを参照する。
     *
     * @param schema スキーマ名
     * @param tableName テーブル名
     * @param connection {@link Connection}
     * @return {@link TableDescriptor}
     */
    public TableDescriptor getTableDescriptor(String schema, String tableName, Connection connection) {
        if (StringUtil.isNullOrEmpty(tableName) || connection == null) {
            throw new IllegalArgumentException("tableName or connection is null or empty.");
        }

        String key = addSchema(schema, tableName);
        TableDescriptor tableDescriptor = tableDescriptorMap.get(key);
        if (tableDescriptor == null) {
            synchronized (this) {
                ResultSetMetaData metaData = getMetaData(schema, tableName, connection);
                tableDescriptor = createTableDescriptor(tableName, metaData);
                tableDescriptorMap.put(key, tableDescriptor);
            }
        }
        return tableDescriptor;
    }

    /**
     * 指定したテーブルのカラムに対応する{@link ColumnDescriptor}を取得する。
     * スキーマ名を指定しなかった場合、デフォルトスキーマを参照する。
     *
     * @param schema スキーマ名
     * @param tableName テーブル名
     * @param columnName カラム名
     * @param connection {@link Connection}
     * @return {@link ColumnDescriptor}
     */
    public ColumnDescriptor getColumnDescriptor(String schema, String tableName, String columnName, Connection connection) {
        if (StringUtil.isNullOrEmpty(tableName) || StringUtil.isNullOrEmpty(columnName) || connection == null) {
            throw new IllegalArgumentException("tableName, columnName or connection is null or empty.");
        }

        TableDescriptor tableDescriptor = getTableDescriptor(schema, tableName, connection);
        return tableDescriptor.getColumnDescriptor(columnName);
    }

    private TableDescriptor createTableDescriptor(String tableName, ResultSetMetaData meta) {
        try {
            boolean caseSensitive = meta.isCaseSensitive(1);
            Map<String, ColumnDescriptor> columnDescriptors = createColumnDescriptors(meta);
            return new TableDescriptor(tableName, caseSensitive, columnDescriptors);
        } catch (SQLException e) {
            throw new DbAccessException("Can not access to metadata. tablename = " + tableName, e);
        }
    }

    private Map<String, ColumnDescriptor> createColumnDescriptors(ResultSetMetaData meta) throws SQLException {
        Map<String, ColumnDescriptor> ret = new HashMap<String, ColumnDescriptor>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String columnName = meta.getColumnName(i);
            int columnType = meta.getColumnType(i);
            ret.put(columnName, new ColumnDescriptor(columnName, columnType));
        }
        return ret;
    }

    /**
     * 指定したテーブルのメタ情報をデータベースから取得する。
     * スキーマ名を指定しなかった場合、デフォルトスキーマを参照する。
     *
     * @param schema スキーマ名
     * @param tableName テーブル名
     * @param connection {@link Connection}
     * @return {@link ResultSetMetaData}
     */
    private ResultSetMetaData getMetaData(String schema, String tableName, Connection connection) {
        // メタデータ取得用のSQL
        String sql = "SELECT * FROM " + addSchema(schema, tableName) + " WHERE 1 = 0";
        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            return ps.executeQuery().getMetaData();
        } catch (SQLException e) {
            throw new DbAccessException("Can not access to metadata. tablename = " + tableName, e);
        }
    }

    private String addSchema(String schema, String tableName) {
        return StringUtil.hasValue(schema) ? schema + "." + tableName : tableName;
    }
}