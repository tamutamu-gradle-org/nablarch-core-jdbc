package nablarch.core.db.connection;

import java.sql.SQLException;

import nablarch.core.db.DbAccessException;
import nablarch.core.util.annotation.Published;

/**
 * SQL文実行時例外の内容に応じて、{@link DbAccessException}を生成するインタフェース。
 * <p/>
 * 実装クラスでは{@link SQLException}の内容を元に、{@link DbAccessException}を生成すること。
 *
 * @author Kiyohito Itoh
 */
@Published(tag = "architect")
public interface DbAccessExceptionFactory {

    /**
     * 発生したSQL実行時例外の内容に応じた{@link DbAccessException}を生成する。
     *
     * @param message エラーメッセージ
     * @param cause 発生した{@link SQLException}
     * @param connection 例外発生時のデータベース接続
     * @return 発生したSQL実行時例外の内容に応じた{@link DbAccessException}
     */
    DbAccessException createDbAccessException(String message, SQLException cause, TransactionManagerConnection connection);
}
