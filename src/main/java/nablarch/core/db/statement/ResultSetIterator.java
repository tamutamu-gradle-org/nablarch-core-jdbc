package nablarch.core.db.statement;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import nablarch.core.db.DbAccessException;
import nablarch.core.util.annotation.Published;

/**
 * {@link ResultSet}のWrapperクラス。<br>
 * <br>
 * 本クラスでは、ResultSetから1行分のデータを{@link SqlRow}で取得するインタフェースを提供する。
 * 以下に例を示す。
 * <code>
 * <pre>
 * ResultSetIterator rs = statement.executeQuery();
 * // for-each文を使って、１レコード文のデータを取得する。
 * for (SqlRow row : rs) {
 *     // SqlRowから各カラムの値を取得し必要な処理を行う。
 * }
 * </pre>
 * </code>
 *
 * @author hisaaki sioiri
 * @see ResultSet
 */
public class ResultSetIterator implements Iterable<SqlRow> {

    /**
     * ResultSet
     */
    private ResultSet rs;

    /**
     * カラム名リスト(大文字に変換したもの)
     */
    private String[] colNames;

    /**
     * カラム名とデータタイプとのMap
     */
    private Map<String, Integer> colTypeMap;

    /**
     * ResultSetConvertorで変換するカラムか否か
     */
    private boolean[] convertCols;

    /**
     * ResultSetMetaData
     */
    private ResultSetMetaData metaData;

    /**
     * ResultSetConvertor
     */
    private ResultSetConvertor convertor;

    /**
     * Iteratorの生成フラグ。<br>
     */
    private boolean makeIterator;

    private SqlStatement statement;

    /**
     * 自身を生成した{@link SqlStatement}を設定する。
     *
     * @param statement ステートメント
     */
    public void setStatement(SqlStatement statement) {
        this.statement = statement;
    }

    /**
     * Statementを取得する。
     *
     * @return この結果セットを生成したStatement
     */
    public SqlStatement getStatement() {
        return statement;
    }

    /**
     * パラメータで指定された{@link ResultSet}を保持するResultSetIteratorオブジェクトを生成する。
     *
     * @param rs ResultSet
     * @param convertor ResultSetConvertor
     */
    public ResultSetIterator(ResultSet rs, ResultSetConvertor convertor) {
        this.rs = rs;
        this.convertor = convertor;

        Map<String, Integer> tmpColTypeMap = new HashMap<String, Integer>();
        try {
            metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            colNames = new String[columnCount];
            convertCols = new boolean[columnCount];

            for (int i = 1; i <= columnCount; i++) {
                String colName = metaData.getColumnLabel(i)
                        .toUpperCase();

                colNames[i - 1] = colName;
                tmpColTypeMap.put(colName, metaData.getColumnType(i));

                // ResultSetConvertorでの変換対象かを判定する。
                if (convertor != null) {
                    convertCols[i - 1] = convertor.isConvertible(metaData, i);
                }
            }
            colTypeMap = Collections.unmodifiableMap(tmpColTypeMap);
        } catch (SQLException e) {
            throw new DbAccessException("failed to initialization.", e);
        }
    }

    /**
     * {@link ResultSet#next()} を行う。
     *
     * @return 次のレコードが存在する場合は{@code true}、存在しない場合は{@code false}
     * @throws DbAccessException {@link SQLException}が発生した場合
     */
    @Published
    public boolean next() {
        try {
            return rs.next();
        } catch (SQLException e) {
            throw new DbAccessException("ResultSet fetch failed.", e);
        }
    }

    /**
     * カレント行の指定されたカラムの値を{@link Object}で取得する。
     *
     * @param columnIndex カラムインデックス
     * @return カラムの値
     * @throws DbAccessException {@link SQLException}が発生した場合
     * @see ResultSet#getObject(int)
     */
    @Published
    public Object getObject(int columnIndex) {
        try {
            return rs.getObject(columnIndex);
        } catch (SQLException e) {
            throw new DbAccessException("failed to getObject. column index = [" + columnIndex + ']', e);
        }
    }

    /**
     * カレント行の指定されたカラムの値を{@link String}で取得する。
     *
     * @param columnIndex カラムインデックス
     * @return カラムの値
     * @throws DbAccessException {@link SQLException}が発生した場合
     * @see ResultSet#getString(int)
     */
    @Published
    public String getString(int columnIndex) {
        try {
            return rs.getString(columnIndex);
        } catch (SQLException e) {
            throw new DbAccessException("failed to getString. column index = [" + columnIndex + ']', e);
        }
    }

    /**
     * カレント行の指定されたカラムの値を{@link Integer}で取得する。
     * <p/>
     * {@link ResultSet#getInt(int)}は、カラムの値が{@code null}の場合は{@code 0}を返すが、
     * 本メソッドではカラムの値が{@code null}の場合は{@code null}を返す。
     * <p/>
     * カラムの値がIntegerに変換可能な場合は、Integerに変換し返却する。<br/>
     * 変換できない場合は、カラムの値を持つ新しいIntegerインスタンスを返却する。
     *
     * @param columnIndex カラムインデックス
     * @return カラムの値
     * @throws NumberFormatException カラムの値をIntegerに変換できなかった場合
     * @see #getObject(int)
     */
    @Published
    public Integer getInteger(int columnIndex) {
        final Object object = getObject(columnIndex);
        if (object == null) {
            return null;
        }
        if (object instanceof Integer) {
            return Integer.class.cast(object);
        }
        return Integer.valueOf(object.toString());
    }

    /**
     * カレント行の指定されたカラムの値を{@link Long}で取得する。
     * <p/>
     * {@link ResultSet#getLong(int)}は、カラムの値が{@code null}の場合は{@code 0}を返すが、
     * 本メソッドではカラムの値が{@code null}の場合は{@code null}を返す。
     * <p/>
     * カラムの値がLongに変換可能な場合は、Longに変換し返却する。<br/>
     * 変換できない場合は、カラムの値を持つ新しいLongインスタンスを返却する。
     *
     * @param columnIndex カラムインデックス
     * @return カラムの値
     * @throws NumberFormatException カラムの値をLongに変換できなかった場合
     * @see #getObject(int)
     */
    @Published
    public Long getLong(int columnIndex) {
        final Object object = getObject(columnIndex);
        if (object == null) {
            return null;
        }
        if (object instanceof Long) {
            return Long.class.cast(object);
        }
        return Long.valueOf(object.toString());
    }

    /**
     * カレント行の指定されたカラムの値を{@link Short}で取得する。
     * <p/>
     * {@link ResultSet#getShort(int)}は、カラムの値が{@code null}の場合は{@code 0}を返すが、
     * 本メソッドではカラムの値が{@code null}の場合は{@code null}を返す。
     * <p/>
     * カラムの値がShortに変換可能な場合は、Shortに変換し返却する。<br/>
     * 変換できない場合は、カラムの値を持つ新しいShortインスタンスを返却する。
     *
     * @param columnIndex カラムインデックス
     * @return カラムの値
     * @throws NumberFormatException カラムの値をShortに変換できなかった場合
     * @see #getObject(int)
     */
    @Published
    public Short getShort(int columnIndex) {
        final Object object = getObject(columnIndex);
        if (object == null) {
            return null;
        }
        if (object instanceof Short) {
            return Short.class.cast(object);
        }
        return Short.valueOf(object.toString());
    }

    /**
     * カレント行の指定されたカラムの値を{@link BigDecimal}で取得する。
     *
     * @param columnIndex カラムインデックス
     * @return カラムの値
     * @throws DbAccessException {@link SQLException}が発生した場合
     * @see ResultSet#getBigDecimal(int)
     */
    @Published
    public BigDecimal getBigDecimal(int columnIndex) {
        try {
            return rs.getBigDecimal(columnIndex);
        } catch (SQLException e) {
            throw new DbAccessException("failed to getBigDecimal. column index = [" + columnIndex + ']', e);
        }
    }

    /**
     * カレント行の指定されたカラムの値を{@link Date}で取得する。
     *
     * @param columnIndex カラムインデックス
     * @return カラムの値
     * @throws DbAccessException {@link SQLException}が発生した場合
     * @see ResultSet#getDate(int)
     */
    @Published
    public Date getDate(int columnIndex) {
        try {
            return rs.getDate(columnIndex);
        } catch (SQLException e) {
            throw new DbAccessException("failed to getDate. column index = [" + columnIndex + ']', e);
        }
    }

    /**
     * カレント行の指定されたカラムの値を{@link Timestamp}で取得する。
     *
     * @param columnIndex カラムインデックス
     * @return カラムの値
     * @throws DbAccessException {@link SQLException}が発生した場合
     * @see ResultSet#getTimestamp(int)
     */
    @Published
    public Timestamp getTimestamp(int columnIndex) {
        try {
            return rs.getTimestamp(columnIndex);
        } catch (SQLException e) {
            throw new DbAccessException("failed to getTimestamp. column index = [" + columnIndex + ']', e);
        }
    }

    /**
     * カレント行の指定されたカラムの値をbyte配列で取得する。
     *
     * @param columnIndex カラムインデックス
     * @return カラムの値
     * @throws DbAccessException {@link SQLException}が発生した場合
     * @see ResultSet#getBytes(int)
     */
    @Published
    public byte[] getBytes(int columnIndex) {
        try {
            return rs.getBytes(columnIndex);
        } catch (SQLException e) {
            throw new DbAccessException("failed to getBytes. column index = [" + columnIndex + ']', e);
        }
    }

    /**
     * カレント行の指定されたカラムの値を{@link Blob}で取得する。
     *
     * @param columnIndex カラムインデックス
     * @return カラムの値
     * @throws DbAccessException {@link SQLException}が発生した場合
     * @see ResultSet#getBlob(int)
     */
    @Published
    public Blob getBlob(int columnIndex) {
        try {
            return rs.getBlob(columnIndex);
        } catch (SQLException e) {
            throw new DbAccessException("failed to getBlob. column index = [" + columnIndex + ']', e);
        }
    }


    /**
     * 現在レコードのデータを取得する。
     * <p/>
     * 本メソッドでは、呼び出されるたびに{@link SqlRow}を生成する。
     *
     * @return 現在レコードを保持したSqlRow
     * @throws DbAccessException {@link SQLException}が発生した場合
     */
    @Published
    public SqlRow getRow() {
        Map<String, Object> tmpRow;
        tmpRow = new HashMap<String, Object>((colNames.length * 3) / 2 + 1);
        try {
            for (int i = 0; i < colNames.length; i++) {
                if (convertCols[i]) {
                    tmpRow.put(colNames[i], convertor.convert(rs, metaData, i + 1));
                } else {
                    tmpRow.put(colNames[i], rs.getObject(i + 1));
                }
            }
        } catch (SQLException e) {
            throw new DbAccessException("failed to getRow.", e);
        }
        return new SqlRow(tmpRow, colTypeMap);
    }

    /**
     * {@link ResultSet#close()}を行う。
     *
     * @throws DbAccessException {@link SQLException}が発生した場合
     * @see ResultSet#close()
     */
    public void close() {
        try {
            rs.close();
        } catch (SQLException e) {
            throw new DbAccessException("failed to ResultSet close.", e);
        }
    }


    /**
     * {@link java.sql.ResultSetMetaData}を取得する。
     *
     * @return ResultSetMetaDataオブジェクト
     */
    public ResultSetMetaData getMetaData() {
        return metaData;
    }

    /**
     *  型Tの要素セットの反復子を返す。
     *
     *  @return 反復子
     *  @throws IllegalArgumentException 複数のメソッドから呼び出された場合
     */
    @Published
    public Iterator<SqlRow> iterator() {
        if (makeIterator) {
            throw new IllegalStateException("multiple method call is unsupported.");
        }
        makeIterator = true;
        return new InnerIterator();
    }

    /**
     * Iterator実装。
     */
    private final class InnerIterator implements Iterator<SqlRow> {

        /**
         * 次レコードの有無
         */
        private boolean isNextRecord;

        /**
         * コンストラクタ。
         * １レコード目の存在チェックのみを実装する。
         */
        private InnerIterator() {
            try {
                isNextRecord = rs.next();
            } catch (SQLException e) {
                throw new DbAccessException("failed to generation of Iterator.", e);
            }
        }

        /**
         * 次レコードが存在するか否か。
         *
         * @return 次レコードが存在する場合は{@code true}
         */
        public boolean hasNext() {
            return isNextRecord;
        }

        /**
         * 次レコードを取得する。
         * </p>
         * 次のレコードが存在しない場合、{@link ResultSet#close()}を行う。
         *
         * @return SqlRow
         * @throws DbAccessException {@link SQLException}が発生した場合
         */
        public SqlRow next() {
            try {
                SqlRow sqlRow = getRow();
                isNextRecord = rs.next();
                if (!isNextRecord) {
                    close();
                }
                return sqlRow;
            } catch (SQLException e) {
                throw new DbAccessException("failed to next.", e);
            }
        }

        /**
         * {@inheritDoc}
         * 本メソッドは、サポートしない。
         */
        public void remove() {
            throw new UnsupportedOperationException("remove operation is unsupported.");
        }

    }
}

