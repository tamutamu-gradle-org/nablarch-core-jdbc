package nablarch.core.db.dialect;

import java.sql.SQLException;

import nablarch.core.db.statement.ResultSetConvertor;
import nablarch.core.db.statement.SelectOption;
import nablarch.core.util.annotation.Published;

/**
 * データベースの実装依存の方言を吸収するためのインタフェース。
 *
 * @author hisaaki shioiri
 */
@Published(tag = "architect")
public interface Dialect {

    /**
     * IDENTITY(オートインクリメントカラム)が使用できるか否か。
     * <p/>
     *
     * @return 使用可能な場合は、 {@code true}
     */
    boolean supportsIdentity();

    /**
     * SEQUENCEが使用できるか否か。
     * <p/>
     *
     * @return 使用可能な場合は、 {@code true}
     */
    boolean supportsSequence();

    /**
     * SQL文でのオフセット指定が使用できるか否か
     *
     * @return 使用可能な場合は、{@code true}
     */
    boolean supportsOffset();

    /**
     * SQL例外がトランザクションタイムアウトと判断すべき例外か否か。
     *
     * @param sqlException SQL例外
     * @return トランザクションタイムアウトと判断すべき場合{@code true}
     */
    boolean isTransactionTimeoutError(SQLException sqlException);

    /**
     * SQL例外が一意制約違反による例外か否か。
     * <p/>
     *
     * @param sqlException SQL例外
     * @return SQL例外が一意制約違反の場合{@code true}
     */
    boolean isDuplicateException(SQLException sqlException);

    /**
     * {@link java.sql.ResultSet}から値を取得するための変換クラスを返却する。
     *
     * @return 変換クラス。
     */
    ResultSetConvertor getResultSetConvertor();

    /**
     * シーケンスオブジェクトの次の値を取得するSQL文を構築する。
     * <p/>
     *
     * @param sequenceName シーケンス名
     * @return シーケンスオブジェクトの次の値を取得するSQL文
     */
    String buildSequenceGeneratorSql(String sequenceName);

    /**
     * SQL文をページング用のSQL文に変換する。
     *
     * @param sql SQL文
     * @param selectOption 検索時のオプション
     * @return 変換したSQL文
     */
    String convertPaginationSql(String sql, SelectOption selectOption);

    /**
     * SQL文をレコード数取得用のSQL文に変換する。
     *
     * @param sql SQL文
     * @return 変換したSQL文
     */
    String convertCountSql(String sql);

    /**
     * ping用のSQL文を返す。
     * <p/>
     * データベースへの死活チェックを行うための、ping用SQL文を生成する。
     *
     * @return ping用のSQL文
     */
    String getPingSql();

    /**
     * SQL型をもとにデータベースに出力する値に変換する。
     * @param value 出力する値
     * @param sqlType SQL型
     * @return 変換後の値
     */
    Object convertToDatabase(final Object value, final int sqlType);

    /**
     * データベースに出力する値に変換する。
     * @param value 出力する値
     * @param dbType データベースの型に対応したクラス
     * @param <T> 出力する値の型
     * @param <DB> データベースに出力する型
     * @return 変換後の値
     */
    <T, DB> DB convertToDatabase(final T value, final Class<DB> dbType);

    /**
     * データベースから入力(取得した値)を変換する。
     * @param value 変換する値
     * @param javaType 変換後のクラス
     * @param <T> 変換後の型
     * @return 変換後の値
     */
    <T> T convertFromDatabase(Object value, Class<T> javaType);

}
