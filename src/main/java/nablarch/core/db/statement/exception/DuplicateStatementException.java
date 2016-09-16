package nablarch.core.db.statement.exception;

import java.sql.SQLException;

import nablarch.core.util.annotation.Published;

/**
 * 一意制約違反時に発生する例外クラス。
 *
 * @author Hisaaki Sioiri
 */
@Published
public class DuplicateStatementException extends SqlStatementException {

    /**
     * {@link SQLException}をラップした{@link DuplicateStatementException}を生成する。
     *
     * @param message エラーメッセージ
     * @param e SQLException
     */
    public DuplicateStatementException(final String message, final SQLException e) {
        super(message, e);
    }
}
