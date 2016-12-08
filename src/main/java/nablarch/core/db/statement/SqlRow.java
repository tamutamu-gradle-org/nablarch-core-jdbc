package nablarch.core.db.statement;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

import nablarch.core.db.DbAccessException;
import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.db.dialect.Dialect;
import nablarch.core.util.annotation.Published;
import nablarch.core.util.map.MultipleKeyCaseMap;


/**
 * 簡易取得結果1行分のデータを保持するクラス。
 * <p/>
 * 各カラムの内容は、そのデータ型に対応したgetメソッドにより取得できる。
 * この際、カラム名の大文字/小文字の違い、アンダースコアの有無は区別せず、
 * 同一のカラム名とみなされる。
 * <p/>
 * 例:
 * <ul>
 * <li>USER_NAMEとuser_nameは同一のカラム名とみなされる。(大文字小文字の区別はしないため)
 * <li>USER_NAMEとuserNameは同一のカラム名とみなされる。(アンダースコアの有無は区別しないため)
 * </ul>
 *
 * @author Hisaaki Sioiri
 */
@Published
public class SqlRow extends MultipleKeyCaseMap<Object> {

    /** データベース方言 */
    private final Dialect dialect;

    /** データタイプ情報 */
    private Map<String, Integer> colType;

    /**
     * 指定されたMapを元にオブジェクトを構築する。
     *
     * @param row 1行分のデータを持つMap
     * @param colType カラムタイプ
     */
    public SqlRow(Map<String, Object> row, Map<String, Integer> colType) {
        this(row, colType, new DefaultDialect());
    }

    /**
     * 指定されたMapを元にオブジェクトを構築する。
     *
     * @param row 1行分のデータを持つMap
     * @param colType カラムタイプ
     * @param ignored カラム名の紐付け情報(本引数は使用しない)
     */
    public SqlRow(Map<String, Object> row, Map<String, Integer> colType, Map<String, String> ignored) {
        this(row, colType, new DefaultDialect());
    }

    /**
     * コピー元となる{@code SqlRow}からオブジェクトを構築する。
     * @param orig コピー元となるインスタンス
     */
    protected SqlRow(SqlRow orig) {
        super(orig);
        this.colType = copyValueOf(orig.colType);
        this.dialect = orig.dialect;
    }

    /**
     * 指定されたMapを元にオブジェクトを構築する。
     *
     * @param row 1行分のデータを持つMap
     * @param colType カラムタイプ
     * @param dialect データベースの方言
     */
    public SqlRow(final Map<String, Object> row, final Map<String, Integer> colType, final Dialect dialect) {
        super(row);
        this.dialect = dialect;
        this.colType = colType;
    }

    /**
     * 指定されたカラムの情報を文字列で取得する。
     * @param colName カラム名
     * @return 指定されたカラム名に対応するString型データ
     *          データベースの検索結果が{@code null}の場合には、{@code null}を返却する
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     * @see Dialect#convertFromDatabase(Object, Class) 
     */
    public final String getString(String colName) {
        return getObject(colName, String.class);
    }

    /**
     * 指定されたカラムの情報を{@link Integer}として取得する。
     * @param colName カラム名
     * @return 指定されたカラム名に対応するInteger型データ。
     *          データベースの検索結果が{@code null}の場合には、{@code null}を返却する
     * @throws IllegalStateException データベースから取得したデータの文字列表現が、Integer型として解釈できない場合
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     * @see DefaultDialect#convertFromDatabase(Object, Class) 
     */
    public final Integer getInteger(String colName) {
        return getObject(colName, Integer.class);
    }

    /**
     * 指定されたカラムの情報を{@link Long}として取得する。
     *
     * @param colName カラム名
     * @return 指定されたカラム名に対応するLong型データ。
     *          データベースの検索結果が{@code null}の場合には、{@code null}を返却する
     * @throws IllegalStateException データベースから取得したデータの文字列表現が、Long型として解釈できない場合
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     * @see Dialect#convertFromDatabase(Object, Class) 
     */
    public final Long getLong(String colName) {
        return getObject(colName, Long.class);
    }

    /**
     * 指定されたカラムの情報を{@link Boolean}として取得する。
     *
     * @param colName カラム名
     * @return {@code true} or {@code false}を返却する。
     *          データベースの検索結果が{@code null}の場合には、{@code null}を返却する
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     * @throws IllegalStateException データベースから取得したデータがBoolean型として解釈できない場合
     * @see Dialect#convertFromDatabase(Object, Class) 
     */
    public Boolean getBoolean(String colName) {
        return getObject(colName, Boolean.class);
    }

    /**
     * 指定されたカラムの情報を{@link BigDecimal}として取得する。
     *
     * @param colName カラム名
     * @return 指定されたカラム名に対応するBigDecimal型データ。
     *          データベースの検索結果が{@code null}の場合には、{@code null}を返却する
     * @throws IllegalStateException データベースから取得したデータが、{@link BigDecimal}として解釈できない場合
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     * @see Dialect#convertFromDatabase(Object, Class) 
     */
    public final BigDecimal getBigDecimal(String colName) {
        return getObject(colName, BigDecimal.class);
    }

    /**
     * 指定されたカラムの情報を{@link Date}として取得する。
     * @param colName カラム名
     * @return 指定されたカラム名に対応するjava.util.Date型データ。
     *          データベースの検索結果が{@code null}の場合には、{@code null}を返却する
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     * @throws IllegalStateException データベースから取得したデータがjava.util.Date型として解釈できない場合
     * @see Dialect#convertFromDatabase(Object, Class) 
     */
    public Date getDate(String colName) {
        return getObject(colName, Date.class);
    }

    /**
     * 指定されたカラムの情報を{@link Timestamp}として取得する。
     * @param colName カラム名
     * @return 指定されたカラム名に対応するjava.sql.Timestamp型データ
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     * @throws IllegalStateException データベースから取得したデータがjava.sql.Timestamp型として解釈できない場合
     * @see Dialect#convertFromDatabase(Object, Class) 
     */
    public Timestamp getTimestamp(String colName) {
        return getObject(colName, Timestamp.class);
    }

    /**
     * 指定されたカラムの情報をbyte配列として取得する。
     * <p/>
     * @param colName カラム名
     * @return 指定されたカラム名に対応するbyte配列データ。
     *          データベースの検索結果が{@code null}の場合には、{@code null}を返却する
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     * @throws DbAccessException データタイプが{@code BLOB}型である場合で、データの読み込みに失敗した場合
     * @throws IllegalStateException データベースから取得したデータがbyte配列として解釈できない場合
     * @see Dialect#convertFromDatabase(Object, Class) 
     */
    public byte[] getBytes(String colName) {
        return getObject(colName, byte[].class);
    }

    /**
     * 指定されたカラムの情報を指定された型として取得する。
     *
     * @param colName カラム名
     * @param javaType 取得するクラス
     * @param <T> 取得する型
     * @return 指定されたカラムを指定のクラスに変換した値
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     * @throws DbAccessException データタイプが{@code BLOB}型である場合で、データの読み込みに失敗した場合
     * @throws IllegalStateException データベースから取得したデータが指定の型として解釈できない場合
     * @see Dialect#convertFromDatabase(Object, Class)
     */
    public <T> T getObject(final String colName, final Class<T> javaType) {
        final Object object = getObject(colName);
        try {
            return dialect.convertFromDatabase(object, javaType);
        } catch (DbAccessException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IllegalStateException("Attribute convertor for dialect is not supported. column name = [" + colName + "], data type = [" + javaType.getSimpleName() + "].", e);
        }
    }

    /**
     * 指定されたカラムの情報を{@link Object}オブジェクトとして取得する。
     *
     * @param colName カラム名
     * @return 指定されたカラム名に対応するデータ。
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     */
    private Object getObject(String colName) {
        if (!containsKey(colName)) {
            throw new IllegalArgumentException("column name = [" + colName + "] is not found");
        }
        return get(colName);
    }

    /**
     * 指定されたカラム名のカラムタイプ({@link java.sql.Types})を取得する。
     *
     * @param colName カラム名
     * @return カラムタイプ
     */
    protected int getColType(String colName) {
        return colType.get(getActualDataKey(colName));
    }
}

