package nablarch.core.db.statement;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import nablarch.core.db.statement.exception.SqlStatementException;
import nablarch.core.util.annotation.Published;


/**
 * バインド変数をもつSQL文を実行するインタフェース。<br>
 *
 * @author Hisaaki Sioiri
 * @see java.sql.PreparedStatement
 */
@Published
public interface SqlPStatement extends SqlStatement {

    /**
     * 簡易検索機能。
     * 下記設定で検索を実行する。
     * <ul>
     *     <li>読み込み開始位置 = 1</li>
     *     <li>最大行数 = 無制限</li>
     * </ul>
     * 本メソッドを使用すると{@link #setMaxRows}で事前に設定した値は無視する。
     *
     * @return 取得結果
     * @throws SqlStatementException SQL実行時に{@link java.sql.SQLException}が発生した場合。
     */
    SqlResultSet retrieve() throws SqlStatementException;

    /**
     * 簡易検索機能。
     *
     * @param start 取得開始位置
     * @param max 取得最大件数
     * @return 取得結果
     * @throws SqlStatementException SQL実行時に{@link java.sql.SQLException}が発生した場合。
     */
    SqlResultSet retrieve(int start, int max) throws SqlStatementException;

    /**
     * {@link java.sql.PreparedStatement#executeQuery}のラッパー。
     *
     * @return 取得結果
     * @throws SqlStatementException SQL実行時に{@link java.sql.SQLException}が発生した場合。
     */
    ResultSetIterator executeQuery() throws SqlStatementException;

    /**
     * {@link java.sql.PreparedStatement#executeUpdate}のラッパー。
     *
     * @return 更新件数
     * @throws SqlStatementException SQL実行時に{@link java.sql.SQLException}が発生した場合。
     */
    int executeUpdate() throws SqlStatementException;

    /**
     * {@link java.sql.PreparedStatement#setNull}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param sqlType SQLタイプ({@link java.sql.Types})
     */
    void setNull(int parameterIndex, int sqlType);

    /**
     * {@link java.sql.PreparedStatement#setBoolean}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setBoolean(int parameterIndex, boolean x);

    /**
     * {@link java.sql.PreparedStatement#setByte}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setByte(int parameterIndex, byte x);

    /**
     * {@link java.sql.PreparedStatement#setShort}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setShort(int parameterIndex, short x);

    /**
     * {@link java.sql.PreparedStatement#setInt}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setInt(int parameterIndex, int x);

    /**
     * {@link java.sql.PreparedStatement#setLong}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setLong(int parameterIndex, long x);

    /**
     * {@link java.sql.PreparedStatement#setFloat}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setFloat(int parameterIndex, float x);

    /**
     * {@link java.sql.PreparedStatement#setDouble}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setDouble(int parameterIndex, double x);

    /**
     * {@link java.sql.PreparedStatement#setBigDecimal}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setBigDecimal(int parameterIndex, BigDecimal x);

    /**
     * {@link java.sql.PreparedStatement#setString}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setString(int parameterIndex, String x);

    /**
     * {@link java.sql.PreparedStatement#setBytes}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setBytes(int parameterIndex, byte[] x);

    /**
     * {@link java.sql.PreparedStatement#setDate}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setDate(int parameterIndex, Date x);

    /**
     * {@link java.sql.PreparedStatement#setTime}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setTime(int parameterIndex, Time x);

    /**
     * {@link java.sql.PreparedStatement#setTimestamp}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setTimestamp(int parameterIndex, Timestamp x);

    /**
     * {@link java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream, int)}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     * @param length ストリームのバイト数
     */
    void setAsciiStream(int parameterIndex, InputStream x, int length);

    /**
     * {@link java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream, int)}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     * @param length ストリームのバイト数
     */
    void setBinaryStream(int parameterIndex, InputStream x, int length);

    /** {@link java.sql.PreparedStatement#clearParameters}のラッパー。 */
    void clearParameters();

    /**
     * {@link java.sql.PreparedStatement#setObject}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     * @param targetSqlType SQLタイプ(<code>java.sql.Types</code>)
     */
    void setObject(int parameterIndex, Object x, int targetSqlType);

    /**
     * {@link java.sql.PreparedStatement#setObject}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setObject(int parameterIndex, Object x);

    /**
     * {@link java.sql.PreparedStatement#execute}のラッパー。
     *
     * @return
     *   最初の結果が{@link java.sql.ResultSet}オブジェクトの場合は{@code true}。
     *   更新カウントであるか、または結果がない場合は{@code false}。
     * @throws SqlStatementException 例外発生時
     */
    boolean execute() throws SqlStatementException;

    /** {@link java.sql.PreparedStatement#addBatch}のラッパー。 */
    void addBatch();

    /**
     * {@link java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader, int)}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param reader パラメータ
     * @param length ストリームないの文字数
     */
    void setCharacterStream(int parameterIndex, Reader reader, int length);

    /**
     * {@link java.sql.PreparedStatement#setRef}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setRef(int parameterIndex, Ref x);

    /**
     * {@link java.sql.PreparedStatement#setBlob}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setBlob(int parameterIndex, Blob x);

    /**
     * {@link java.sql.PreparedStatement#setClob}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setClob(int parameterIndex, Clob x);

    /**
     * {@link java.sql.PreparedStatement#setArray}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setArray(int parameterIndex, Array x);

    /**
     * {@link java.sql.PreparedStatement#getMetaData}のラッパー。
     *
     * @return ResultSetMetaData
     */
    ResultSetMetaData getMetaData();

    /**
     * {@link java.sql.PreparedStatement#setDate(int, java.sql.Date, java.util.Calendar)}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     * @param cal ドライバが日付を作成するために使用する{@link java.util.Calendar}オブジェクト
     */
    void setDate(int parameterIndex, Date x, Calendar cal);

    /**
     * {@link java.sql.PreparedStatement#setTime(int, java.sql.Time, java.util.Calendar)}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     * @param cal ドライバが日付を作成するために使用する{@link java.util.Calendar}オブジェクト
     */
    void setTime(int parameterIndex, Time x, Calendar cal);

    /**
     * {@link java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp, java.util.Calendar)}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     * @param cal ドライバが日付を作成するために使用する{@link java.util.Calendar}オブジェクト
     */
    void setTimestamp(int parameterIndex, Timestamp x, Calendar cal);

    /**
     * {@link java.sql.PreparedStatement#setNull(int, int, String)}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param sqlType SQLタイプ
     * @param typeName SQL ユーザー定義型の完全指定の名前。
     *                 パラメータがユーザー定義型でも{@link java.sql.Ref}でもない場合は無視される。
     */
    void setNull(int parameterIndex, int sqlType, String typeName);

    /**
     * {@link java.sql.PreparedStatement#setURL}のラッパー。
     *
     * @param parameterIndex パラメータインデックス
     * @param x パラメータ
     */
    void setURL(int parameterIndex, URL x);

    /**
     * {@link java.sql.PreparedStatement#getResultSet}のラッパー。
     *
     * @return
     *   {@link java.sql.ResultSet}オブジェクトとしての現在の結果。
     *   更新カウントであるか、結果がない場合は{@code null}。
     */
    ResultSet getResultSet();

    /**
     * {@link java.sql.PreparedStatement#getMoreResults}のラッパー。
     *
     * @return
     *   次の結果が{@link java.sql.ResultSet}オブジェクトの場合は{@code true}。
     *   更新カウントであるか、結果がない場合は{@code false}。
     */
    boolean getMoreResults();

    /**
     * {@link java.sql.PreparedStatement#setFetchDirection}のラッパー。
     *
     * @param direction 行を処理する初期方向
     */
    void setFetchDirection(int direction);

    /**
     * {@link java.sql.PreparedStatement#getFetchDirection}のラッパー。
     *
     * @return この Statement オブジェクトから生成された結果セットのデフォルトのフェッチ方向
     */
    int getFetchDirection();

    /**
     * {@link java.sql.PreparedStatement#getResultSetConcurrency}のラッパー。
     *
     * @return
     *   {@link java.sql.ResultSet#CONCUR_READ_ONLY}
     *   または{@link java.sql.ResultSet#CONCUR_UPDATABLE}。
     */
    int getResultSetConcurrency();

    /**
     * {@link java.sql.PreparedStatement#getResultSetType}のラッパー。
     *
     * @return
     *   {@link java.sql.ResultSet#TYPE_FORWARD_ONLY}、
     *   {@link java.sql.ResultSet#TYPE_SCROLL_INSENSITIVE}、
     *   {@link java.sql.ResultSet#TYPE_SCROLL_SENSITIVE}のうちの1つ。
     */
    int getResultSetType();

    /**
     * {@link java.sql.PreparedStatement#getMoreResults}のラッパー。
     *
     * @param current getResultSet メソッドを使用して取得した、
     *                現在の {@link java.sql.ResultSet} オブジェクトに生じる状態を示す Statement 定数。
     *                {@link java.sql.Statement#CLOSE_CURRENT_RESULT}、
     *                {@link java.sql.Statement#KEEP_CURRENT_RESULT}、
     *                {@link java.sql.Statement#CLOSE_ALL_RESULTS}のうちの 1 つ。
     * @return
     *   次の結果が{@link java.sql.ResultSet}オブジェクトの場合は{@code true}。
     *   更新カウントであるか、または結果がない場合は{@code false}。
     */
    boolean getMoreResults(int current);

    /**
     * {@link java.sql.PreparedStatement#getGeneratedKeys}のラッパー。
     *
     * @return この Statement オブジェクトの実行で生成された自動生成キーを含む{@link java.sql.ResultSet}オブジェクト
     */
    ResultSet getGeneratedKeys();

    /**
     * {@link java.sql.PreparedStatement#getResultSetHoldability}のラッパー。
     *
     * @return
     *   {@link java.sql.ResultSet#HOLD_CURSORS_OVER_COMMIT}
     *   または{@link java.sql.ResultSet#CLOSE_CURSORS_AT_COMMIT}。
     */
    int getResultSetHoldability();

}

