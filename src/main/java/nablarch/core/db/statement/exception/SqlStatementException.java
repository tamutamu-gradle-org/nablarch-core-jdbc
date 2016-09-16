package nablarch.core.db.statement.exception;

import java.sql.SQLException;

import nablarch.core.db.DbAccessException;
import nablarch.core.util.annotation.Published;


/**
 * SQL文実行時に発生する例外クラス。
 *
 * @author Hisaaki Sioiri
 */
@Published
public class SqlStatementException extends DbAccessException {

    /**
     * {@code SqlStatementException}オブジェクトを生成する。
     *
     * @param message エラーメッセージ
     * @param e SQLException
     */
    public SqlStatementException(final String message, final SQLException e) {
        super(message, e);
    }
}
