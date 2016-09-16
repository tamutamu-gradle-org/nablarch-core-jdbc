package nablarch.core.db.connection.exception;

import java.sql.SQLException;

import nablarch.core.db.DbAccessException;
import nablarch.core.util.annotation.Published;
import nablarch.fw.handler.retry.Retryable;


/**
 * データベース接続に関する問題が発生した場合に送出される例外。
 *
 * @author Kiyohito Itoh
 */
@Published
public class DbConnectionException extends DbAccessException implements Retryable {

    /**
     * DbConnectionExceptionを生成する。
     *
     * @param message エラーメッセージ
     * @param e SQL例外
     */
    public DbConnectionException(String message, SQLException e) {
        super(message, e);
    }
}
