package nablarch.core.db.statement;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

import nablarch.core.db.DbAccessException;
import nablarch.core.util.StringUtil;
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


    //*************************************************************************
    // 本クラスの内部構造を下記に示す。
    //
    // ◆実行するSQL文
    //   SELECT USER_ID, USER_NAME FROM USR
    //
    // ◆上記SQL文を実行した場合の本クラスの状態
    //  KEY→select句に記述されたカラム名を大文字変換した値
    //  VALUE→select文で取得した値
    //  +------------+-------------------+
    //  | KEY        | VALUE             |
    //  +------------+-------------------+
    //  | USER_ID    | 00001             |
    //  | USER_NAME  | ユーザ名          |
    //  +------------+-------------------+
    //
    // ◆カラム名変換用Map(convertedColNames)の状態
    //  KEY→select句に記述されたカラム名を小文字変換し、アンダースコアを取り除いた値
    //  VALUE→select句に記述されたカラム名を大文字変換した値
    //  +-----------+-------------------+
    //  | KEY       | VALUE             |
    //  +-----------+-------------------+
    //  | userid    | USER_ID           |
    //  | username  | USER_NAME         |
    //  +-----------+-------------------+
    //
    // ◆カラム名変換用Map(convertedColNames)は、1度使用されたカラム名をキャッシュするため、状態が変化する。
    //  getString("user_id");が呼び出されると
    //  ①user_idをキーにして、本MapからVALUE値を取得し、カラム名とする。
    //  ②①で値が取得できない場合は、小文字変換とアンダースコアの除去を行い(useridをキーに)再度本Mapから値を取得し、カラム名とする。
    //    user_idが再度指定された場合に、変換処理が行われないようにここで指定された値を本Mapに追加する。
    //  ③パラメータで指定されたuser_idをカラム名とする。
    //  ④上記①、②、③で取得した値を元に、本クラスのMapから値を取得する。(user_idから変換されたUSER_IDを元に値が取得されるため、「00001」が取得される。
    //  +-----------+-------------------+
    //  | KEY       | VALUE             |
    //  +-----------+-------------------+
    //  | userid    | USER_ID           |
    //  | username  | USER_NAME         |
    //  | user_id   | USER_ID           |→②の処理で追加された要素
    //  +-----------+-------------------+
    //
    //  get("userName");が呼ばれると、USER_NAMEをカラム名として取得するため「ユーザ名」が取得される。
    //  +-----------+-------------------+
    //  | KEY       | VALUE             |
    //  +-----------+-------------------+
    //  | userid    | USER_ID           |
    //  | username  | USER_NAME         |
    //  | user_id   | USER_ID           |
    //  | userName  | USER_NAME         |→get("userName")で新たに追加される要素
    //  +-----------+-------------------+
    //*************************************************************************

    /** データタイプ情報 */
    private Map<String, Integer> colType;

    /**
     * 指定されたMapを元にオブジェクトを構築する。
     *
     * @param row 1行分のデータを持つMap
     * @param colType カラムタイプ
     */
    public SqlRow(Map<String, Object> row, Map<String, Integer> colType) {
        super(row);
        this.colType = colType;
    }

    /**
     * 指定されたMapを元にオブジェクトを構築する。
     *
     * @param row 1行分のデータを持つMap
     * @param colType カラムタイプ
     * @param ignored カラム名の紐付け情報(本引数は使用しない)
     */
    public SqlRow(Map<String, Object> row, Map<String, Integer> colType, Map<String, String> ignored) {
        super(row);
        this.colType = colType;
    }

    /**
     * コピー元となる{@code SqlRow}からオブジェクトを構築する。
     * @param orig コピー元となるインスタンス
     */
    protected SqlRow(SqlRow orig) {
        super(orig);
        this.colType = copyValueOf(orig.colType);
    }

    /**
     * 指定されたカラムの情報を文字列で取得する。
     * @param colName カラム名
     * @return 指定されたカラム名に対応するString型データ(toString()した結果を返却する)。
     *          データベースの検索結果が{@code null}の場合には、{@code null}を返却する
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     */
    public final String getString(String colName) {
        Object o = getObject(colName);
        return o == null ? null : StringUtil.toString(o);
    }

    /**
     * 指定されたカラムの情報を{@link Integer}として取得する。
     * <p/>
     * データベースから取得したデータがInteger型である場合、その値をそのまま返却する。<br/>
     * それ以外の型の場合、そのデータの文字列表現(toString()した結果)を、
     * {@link Integer#valueOf(String)}を使用してInteger型に変換し返却する。
     * <p/>
     * データベースから取得したデータがどのような文字列表現を返却するかは、
     * 使用するRDBMSのJDBCドライバに依存する。
     * <p/>
     * 以下に例を示す。
     * <pre>
     * | 文字列表現 | 結果                  |
     * |------------+-----------------------|
     * | "1"        |                     1 |
     * | "-1"       |                    -1 |
     * |"2147483648"| NumberFormatException |
     * | "1.0"      | NumberFormatException |
     * | "ABC"      | NumberFormatException |
     * </pre>
     *
     * @param colName カラム名
     * @return 指定されたカラム名に対応するInteger型データ。
     *          データベースの検索結果が{@code null}の場合には、{@code null}を返却する
     * @throws NumberFormatException データベースから取得したデータの文字列表現が、Integer型として解釈できない場合
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     */
    public final Integer getInteger(String colName) {
        Object o = getObject(colName);
        if (o instanceof Integer) {
            return (Integer) o;
        }
        return o == null ? null : Integer.valueOf(o.toString());
    }

    /**
     * 指定されたカラムの情報を{@link Long}として取得する。
     * <p/>
     * データベースから取得したデータがLong型である場合、その値をそのまま返却する。<br/>
     * それ以外の型の場合、そのデータの文字列表現(toString()した結果)を、
     * {@link Long#valueOf(String)}を使用してLong型に変換し返却する。
     * <p/>
     * データベースから取得したデータがどのような文字列表現を返却するかは、
     * 使用するRDBMSのJDBCドライバに依存する。
     * </p>
     * 以下に例を示す。
     * <pre>
     * | 文字列表現 | 結果                  |
     * |------------+-----------------------|
     * | "1"        |                     1 |
     * | "-1"       |                    -1 |
     * |"2147483648"|            2147483648 |
     * | "1.0"      | NumberFormatException |
     * | "ABC"      | NumberFormatException |
     * </pre>
     *
     * @param colName カラム名
     * @return 指定されたカラム名に対応するLong型データ。
     *          データベースの検索結果が{@code null}の場合には、{@code null}を返却する
     * @throws NumberFormatException データベースから取得したデータの文字列表現が、Long型として解釈できない場合
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     */
    public final Long getLong(String colName) {
        Object o = getObject(colName);
        if (o instanceof Long) {
            return (Long) o;
        }
        return o == null ? null : Long.valueOf(o.toString());
    }

    /**
     * 指定されたカラムの情報を{@link Boolean}として取得する。
     * <p/>
     * 以下の値の場合、{@link Boolean#TRUE}を返却し、それ以外は全て{@link Boolean#FALSE}を返却する。
     * <ul>
     *     <li>booleanの{@code true}の場合</li>
     *     <li>{@link String}の場合で"1" or "on" or "true"の場合(大文字、小文字の区別はしない)</li>
     *     <li>数値型で0以外の場合</li>
     * </ul>
     * <p/>
     * データベースから取得したデータのデータタイプが下記に該当しない場合は、{@link IllegalStateException}を送出する。
     * <ul>
     * <li>{@link Boolean}</li>
     * <li>{@link String}</li>
     * <li>{@link Number}のサブクラス</li>
     * </ul>
     *
     * @param colName カラム名
     * @return {@code true} or {@code false}を返却する。
     *          データベースの検索結果が{@code null}の場合には、{@code null}を返却する
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     * @IllegalStateException データベースから取得したデータがBoolean型として解釈できない場合
     */
    public Boolean getBoolean(String colName) {
        final Object o = getObject(colName);
        if (o == null) {
            return null;
        } else if (o instanceof Boolean) {
            return Boolean.class.cast(o);
        } else if (o instanceof String) {
            final String str = String.class.cast(o);
            if (str.equals("1")
                    || str.equalsIgnoreCase("on")
                    || str.equalsIgnoreCase("true")) {
                return true;
            } else {
                return false;
            }
        } else if (o instanceof Number) {
            final Number number = Number.class.cast(o);
            return number.intValue() != 0;
        } else {
            throw new IllegalStateException("Boolean and incompatible data types. column name = [" + colName + ']');
        }
    }

    /**
     * 指定されたカラムの情報を{@link BigDecimal}として取得する。
     *
     * @param colName カラム名
     * @return 指定されたカラム名に対応するBigDecimal型データ。
     *          データベースの検索結果が{@code null}の場合には、{@code null}を返却する
     * @throws NumberFormatException データベースから取得したデータの文字列表現(toString()した結果)が、BigDecimal型として解釈できない場合
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     */
    public final BigDecimal getBigDecimal(String colName) {
        Object o = getObject(colName);
        if (o instanceof BigDecimal) {
            return (BigDecimal) o;
        }
        return o == null ? null : new BigDecimal(o.toString());
    }

    /**
     * 指定されたカラムの情報を{@link java.util.Date}として取得する。
     * <p/>
     * データベースから取得したデータのデータタイプが下記のデータの場合、java.util.Dateとして取得する
     * 下記に該当しない場合は、{@link IllegalStateException}を送出する。<br>
     * <ul>
     * <li>{@link java.util.Date}</li>
     * <li>{@link java.sql.Timestamp}</li>
     * </ul>
     *
     * @param colName カラム名
     * @return 指定されたカラム名に対応するjava.util.Date型データ。
     *          データベースの検索結果が{@code null}の場合には、{@code null}を返却する
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     * @IllegalStateException データベースから取得したデータがjava.util.Date型として解釈できない場合
     */
    public Date getDate(String colName) {
        Object o = getObject(colName);
        int type = getColType(colName);
        if (type == java.sql.Types.DATE || type == java.sql.Types.TIMESTAMP || o instanceof Timestamp) {
            return o == null ? null : (Date) o;
        }
        throw new IllegalStateException("data is not date type. column name = [" + colName + "]");
    }

    /**
     * 指定されたカラムの情報を{@link java.sql.Timestamp}として取得する。
     * <p/>
     * データベースから取得したデータのデータタイプが下記のデータの場合、{@link java.sql.Timestamp}として取得する。
     * 下記に該当しない場合は、{@link IllegalStateException}を送出する。<br>
     * <ul>
     * <li>{@link java.sql.Timestamp}</li>
     * </ul>
     *
     * @param colName カラム名
     * @return 指定されたカラム名に対応するjava.sql.Timestamp型データ
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     * @IllegalStateException データベースから取得したデータがjava.sql.Timestamp型として解釈できない場合
     */
    public Timestamp getTimestamp(String colName) {
        Object o = getObject(colName);
        int type = getColType(colName);
        if (o instanceof Timestamp || type == java.sql.Types.TIMESTAMP) {
            return (Timestamp) getObject(colName);
        }
        throw new IllegalStateException("data is not timestamp type. column name = [" + colName + "]");
    }

    /**
     * 指定されたカラムの情報をbyte配列として取得する。
     * <p/>
     * データベースから取得したデータのデータタイプが下記のデータの場合、byte配列として取得する。<br>
     * 下記に該当しない場合は、{@link IllegalStateException}を送出する。
     * <ul>
     * <li>{@link java.sql.Types#BLOB}</li>
     * <li>{@link java.sql.Types#BINARY}</li>
     * <li>{@link java.sql.Types#VARBINARY}</li>
     * <li>{@link java.sql.Types#LONGVARBINARY}</li>
     * </ul>
     *
     * @param colName カラム名
     * @return 指定されたカラム名に対応するbyte配列データ。
     *          データベースの検索結果が{@code null}の場合には、{@code null}を返却する
     * @throws IllegalArgumentException 指定されたカラム名が存在しない場合
     * @throws DbAccessException データタイプが{@code BLOB}型である場合で、データの読み込みに失敗した場合
     * @IllegalStateException データベースから取得したデータがbyte配列として解釈できない場合
     */
    public byte[] getBytes(String colName) {
        Object o = getObject(colName);
        int type = getColType(colName);
        if (type == java.sql.Types.BINARY || type == java.sql.Types.VARBINARY || type == java.sql.Types.LONGVARBINARY) {
            return o == null ? null : (byte[]) o;
        } else if (type == java.sql.Types.BLOB) {
            Blob blob = (Blob) o;
            if (o == null) {
                return null;
            }
            try {
                return blob.getBytes(1, (int) blob.length());
            } catch (SQLException e) {
                throw new DbAccessException("failed to getBytes. column name = [" + colName + "]", e);
            }
        }
        throw new IllegalStateException("data is not bytes type. column name = [" + colName + "]");
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

