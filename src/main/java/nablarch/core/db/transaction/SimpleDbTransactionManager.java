package nablarch.core.db.transaction;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.transaction.Transaction;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.transaction.TransactionFactory;
import nablarch.core.util.annotation.Published;


/**
 * データベースへのトランザクション制御を行うクラス。<br>
 *
 * @author Hisaaki Sioiri
 * @see nablarch.core.db.connection.ConnectionFactory
 * @see nablarch.core.transaction.TransactionFactory
 * @see nablarch.core.ThreadContext
 * @see nablarch.core.db.connection.DbConnectionContext
 */
@Published(tag = "architect")
public class SimpleDbTransactionManager {

    /** コネクションファクトリ */
    private ConnectionFactory connectionFactory;

    /** トランザクションファクトリ */
    private TransactionFactory transactionFactory;

    /** トランザクション名 */
    private String dbTransactionName = TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY;

    /** トランザクションを開始する。<br> */
    public void beginTransaction() {

        if (connectionFactory == null) {
            throw new IllegalStateException("ConnectionFactory is not set.");
        }
        if (transactionFactory == null) {
            throw new IllegalStateException("TransactionFactory is not set.");
        }

        if (DbConnectionContext.containConnection(dbTransactionName)) {
            throw new IllegalStateException("The specified database connection name is already used. connection name=[" + dbTransactionName + ']');
        }
        AppDbConnection dbConnection = connectionFactory.getConnection(dbTransactionName);
        DbConnectionContext.setConnection(dbTransactionName, dbConnection);
        try {
            Transaction tran = transactionFactory.getTransaction(dbTransactionName);
            tran.begin();
            TransactionContext.setTransaction(dbTransactionName, tran);
        } catch (Throwable e) {
            // トランザクションの開始処理でエラーが発生した場合は、終了処理を行う。
            try {
                endTransaction();
            } catch (Throwable ignored) { // SUPPRESS CHECKSTYLE ここではトランザクション開始時と同じDB例外が発生する可能性しかないため、例外処理を何も行わない。
                // nop
            }
            throw new RuntimeException("transaction begin failed. dbTransactionName = " + dbTransactionName, e);
        }
    }

    /** トランザクションをコミットする。<br> */
    public void commitTransaction() {
        Transaction transaction = TransactionContext.getTransaction(dbTransactionName);
        transaction.commit();
    }

    /** トランザクションをロールバックする。<br> */
    public void rollbackTransaction() {
        Transaction transaction = TransactionContext.getTransaction(dbTransactionName);
        transaction.rollback();
    }


    /** トランザクションを終了し、リソースを解放する。。<br> */
    public void endTransaction() {
        try {
            TransactionManagerConnection connection = (TransactionManagerConnection) DbConnectionContext.getConnection(
                    dbTransactionName);
            connection.terminate();
        } finally {
            DbConnectionContext.removeConnection(dbTransactionName);
            TransactionContext.removeTransaction(dbTransactionName);
        }
    }

    /**
     * デフォルトのコネクションファクトリクラスを設定する。
     *
     * @param connectionFactory ConnectionFactory
     */
    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * デフォルトのトランザクションファクトリクラスを設定する。
     *
     * @param transactionFactory TransactionFactory
     */
    public void setTransactionFactory(TransactionFactory transactionFactory) {
        this.transactionFactory = transactionFactory;
    }

    /**
     * トランザクション名を設定する。<br>
     * トランザクション名が設定されない場合は、デフォルトでnablarch.core.transaction.TransactionContext#DEFAULT_TRANSACTION_CONTEXT_KEYを使用する。
     *
     * @param dbTransactionName トランザクション名
     */
    public void setDbTransactionName(String dbTransactionName) {
        this.dbTransactionName = dbTransactionName;
    }

    /**
     * トランザクション名を取得する。
     *
     * @return トランザクション名
     */
    public String getDbTransactionName() {
        return dbTransactionName;
    }
}

