package nablarch.core.db.statement.exception;

import java.sql.SQLException;

import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.statement.SqlStatementExceptionFactory;

/**
 * {@link nablarch.core.db.statement.SqlStatementExceptionFactory}のBasic実装クラス。<br>
 * {@link java.sql.SQLException}が一意制約違反の場合には、{@link nablarch.core.db.statement.exception.DuplicateStatementException}を生成する。<br>
 * 一意制約違反以外の場合には、{@link nablarch.core.db.statement.exception.SqlStatementException}を生成する。<br>
 * 一意制約違反の判定には、{@link java.sql.SQLException#getSQLState()}または、{@link java.sql.SQLException#getErrorCode()}を使用する。
 *
 * @author Hisaaki Sioiri
 */
public class BasicSqlStatementExceptionFactory implements SqlStatementExceptionFactory {

    /**
     * {@link SqlStatementException}を生成し返却する。<br>
     * パラメータで指定された{@link SQLException}が一意制約違反の場合には、{@link DuplicateStatementException}を生成する。<br>
     * それ以外の場合には、SqlStatementExceptionを生成する。
     *
     * @param msg メッセージ
     * @param e SQLException
     * @param context DBアクセス時の実行コンテキスト
     * @return 生成したSqlStatementException
     */
    public SqlStatementException createSqlStatementException(String msg, SQLException e, DbExecutionContext context) {
        if (context.getDialect().isDuplicateException(e)) {
            return new DuplicateStatementException(msg, e);
        }
        return new SqlStatementException(msg, e);
    }
}

