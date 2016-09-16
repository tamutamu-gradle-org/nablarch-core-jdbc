package nablarch.core.db.connection.exception;

import java.sql.SQLException;

import nablarch.core.db.DbAccessException;
import nablarch.core.db.connection.DbAccessExceptionFactory;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.dialect.Dialect;

/**
 * {@link DbAccessExceptionFactory}の基本実装クラス。
 * @author Kiyohito Itoh
 */
public class BasicDbAccessExceptionFactory implements DbAccessExceptionFactory {

    /**
     * {@inheritDoc}
     * </p>
     * 発生した例外がデータベース接続に関する問題である場合は、{@link DbConnectionException}を生成する。
     * データベース接続に関する問題でない場合は、{@link DbAccessException}を生成する。
     * </p>
     * 発生した例外がデータベース接続に関する問題であるか否かの判定は、
     * {@link #isDbConnectionError(SQLException, TransactionManagerConnection)}メソッドに委譲する。
     */
    public DbAccessException createDbAccessException(String message, SQLException cause, TransactionManagerConnection connection) {
        if (isDbConnectionError(cause, connection)) {
            return new DbConnectionException(message, cause);
        }
        return new DbAccessException(message, cause);
    }

    /**
     * 発生した例外がデータベース接続に関する問題であるか否かを判定する。
     * <p/>
     * 基本実装では、プロパティに指定されたSQL文と引数に指定されたデータベース接続を使用して、
     * SQL文を実行することにより判定を行う。
     * <p/>
     * 引数に指定されたデータベース接続がnullの場合、
     * またはSQL文の実行で{@link DbAccessException}が送出された場合はtrueを返す。
     * 
     * @param cause 発生した例外
     * @param connection 例外発生時のデータベース接続
     * @return 発生した例外がデータベース接続に関する問題である場合はtrue
     */
    protected boolean isDbConnectionError(SQLException cause, TransactionManagerConnection connection) {
        if (connection == null) {
            return true;
        }
        final Dialect dialect = connection.getDialect();
        try {
            connection.prepareStatement(dialect.getPingSql()).execute();
            return false;
        } catch (DbAccessException e) {
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }
}
