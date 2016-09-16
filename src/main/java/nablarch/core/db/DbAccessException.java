package nablarch.core.db;

import java.sql.SQLException;

import nablarch.core.util.annotation.Published;

/**
 * データベースアクセス時に発生する例外。
 * <p/>
 * データベースアクセス時に{@link java.sql.SQLException}が発生した場合、本クラスでラップし再送出すること。
 *
 * @author Hisaaki Sioiri
 * @see java.sql.SQLException
 */
@Published
public class DbAccessException extends RuntimeException {

    /** SQLException */
    private SQLException se;

    /**
     * 本クラスのインスタンスを生成する。
     *
     * @param message エラーメッセージ
     * @param e データベースアクセス時に送出された{@link SQLException}オブジェクト
     */
    public DbAccessException(final String message, final SQLException e) {
        super(message, e);
        se = e;
    }

    /**
     * SQLState値を取得する。
     *
     * @return SQLState値
     * @see java.sql.SQLException#getSQLState()
     */
    public final String getSQLState() {
        return se.getSQLState();
    }

    /**
     * エラーコードを取得する。
     *
     * @return エラーコード
     * @see java.sql.SQLException#getErrorCode()
     */
    public final int getErrorCode() {
        return se.getErrorCode();
    }
}

