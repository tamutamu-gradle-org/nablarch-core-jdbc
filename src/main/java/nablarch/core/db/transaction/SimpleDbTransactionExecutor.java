package nablarch.core.db.transaction;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.util.annotation.Published;

/**
 * {@link SimpleDbTransactionManager}を使用して簡易的にSQL文を実行するクラス。
 * <br/>
 * 本クラスを継承したクラスは、{@link #execute(nablarch.core.db.connection.AppDbConnection)}を実装し、
 * SQL文の実行を行う。<br/>
 * これにより、{@link SimpleDbTransactionManager}を直接使用するときと比べて、
 * トランザクション管理などを実装する必要がなく、簡易的にSQL文を実行出来るようになる。
 * <br/>
 *
 * @param <T> トランザクション実行結果の型
 * @author hisaaki sioiri
 * @see SimpleDbTransactionManager
 */
@Published(tag = "architect")
public abstract class SimpleDbTransactionExecutor<T> {

    /** トランザクションマネージャ */
    private SimpleDbTransactionManager transactionManager;

    /** Logger */
    private static final Logger LOG = LoggerManager.get(
            SimpleDbTransactionExecutor.class);

    /**
     * コンストラクタ。
     *
     * @param transactionManager トランザクションマネージャ
     */
    public SimpleDbTransactionExecutor(
            SimpleDbTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    /**
     * トランザクションを実行する。<br/>
     *
     * @return トランザクション実行結果
     */
    public T doTransaction() {
        transactionManager.beginTransaction();

        Throwable throwable = null;
        try {
            T result = execute(DbConnectionContext.getConnection(
                    transactionManager.getDbTransactionName()));
            // 正常終了時はコミット処理を行う。
            transactionManager.commitTransaction();
            return result;
        } catch (RuntimeException e) {
            try {
                transactionManager.rollbackTransaction();
            } catch (RuntimeException exception) {
                writeWarnLog(e);
                throwable = exception;
                throw exception;
            } catch (Error error) {
                writeWarnLog(e);
                throwable = error;
                throw error;
            }
            throwable = e;
            throw e;
        } catch (Error e) {
            try {
                transactionManager.rollbackTransaction();
            } catch (RuntimeException exception) {
                writeWarnLog(e);
                throwable = exception;
                throw exception;
            } catch (Error error) {
                writeWarnLog(e);
                throwable = error;
                throw error;
            }
            throwable = e;
            throw e;
        } finally {
            try {
                transactionManager.endTransaction();
            } catch (RuntimeException e) {
                writeWarnLog(throwable);
                throw e;
            } catch (Error e) {
                writeWarnLog(throwable);
                throw e;
            }
        }
    }

    /**
     * ワーニングレベルのログ出力を行う。<br/>
     * 指定されたオリジナル例外がnull以外の場合のみログ出力を行い、
     * nullの場合には、処理は行わない。
     * <p/>
     *
     * @param orgError オリジナル例外
     */
    private static void writeWarnLog(Throwable orgError) {
        if (orgError != null) {
            LOG.logWarn("SimpleDbTransactionExecutor#doTransaction failed in the application process.", orgError);
        }
    }

    /**
     * SQL文を実行する。<br/>
     *
     * @param connection コネクション
     * @return トランザクション実行結果
     */
    public abstract T execute(AppDbConnection connection);
}

