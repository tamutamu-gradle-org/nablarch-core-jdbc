package nablarch.core.db.statement;

import java.sql.SQLException;

import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.statement.exception.SqlStatementException;
import nablarch.core.util.annotation.Published;

/**
 * {@link SqlStatementException}を生成するインタフェース。<br>
 * SQLのエラー内容({@link java.sql.SQLException#getSQLState()}や{@link java.sql.SQLException#getErrorCode()}の結果)に応じて、
 * 生成する{@link SqlStatementException}を切り替える場合には、具象クラスで生成するExceptionの切り替えを行う。
 *
 * @author Hisaaki Sioiri
 */
@Published(tag = "architect")
public interface SqlStatementExceptionFactory {

    /**
     * {@link nablarch.core.db.statement.exception.SqlStatementException}を生成し返却する。
     *
     * @param msg メッセージ
     * @param e SQLException
     * @param context DBアクセス実行コンテキスト
     * @return 生成したSqlStatementException
     */
    SqlStatementException createSqlStatementException(String msg, SQLException e, DbExecutionContext context);
}
