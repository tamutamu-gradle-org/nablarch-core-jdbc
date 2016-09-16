package nablarch.core.db.transaction;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import nablarch.core.transaction.Transaction;
import nablarch.core.transaction.TransactionFactory;


/**
 * {@link nablarch.core.db.transaction.JdbcTransaction}を生成するクラス。
 *
 * @author Hisaaki Sioiri
 */
public class JdbcTransactionFactory implements TransactionFactory {

    /** アイソレーションレベル */
    private int isolationLevel = Connection.TRANSACTION_READ_COMMITTED;

    /** 初期SQL */
    private List<String> initSqlList;

    /** トランザクションタイムアウト秒数 */
    private int transactionTimeoutSec;

    /**
     * {@link nablarch.core.db.transaction.JdbcTransaction}を生成する。
     *
     * @param connectionName コネクション名
     * @return トランザクションオブジェクト
     */
    public Transaction getTransaction(String connectionName) {
        JdbcTransaction transaction = new JdbcTransaction(connectionName);
        transaction.setIsolationLevel(isolationLevel);
        transaction.setInitSqlList(initSqlList == null ? new ArrayList<String>(0) : initSqlList);
        if (transactionTimeoutSec > 0) {
            JdbcTransactionTimeoutHandler timeout = new JdbcTransactionTimeoutHandler(transactionTimeoutSec);
            transaction.setTransactionTimeoutHandler(timeout);
        }
        return transaction;
    }

    /**
     * アイソレーションレベルを設定する。<br>
     * 設定できる値は、下記のとおり。<br>
     * READ_COMMITTED:{@link java.sql.Connection#TRANSACTION_READ_COMMITTED}<br>
     * READ_UNCOMMITTED:{@link java.sql.Connection#TRANSACTION_READ_UNCOMMITTED}<br>
     * REPEATABLE_READ:{@link java.sql.Connection#TRANSACTION_REPEATABLE_READ}<br>
     * SERIALIZABLE:{@link java.sql.Connection#TRANSACTION_SERIALIZABLE}<br>
     * アイソレーションレベルが設定されない場合は、デフォルトで{@link java.sql.Connection#TRANSACTION_READ_COMMITTED}が使用される。
     *
     * @param isolationLevel アイソレーションレベルを表す文字列。
     */
    public void setIsolationLevel(String isolationLevel) {
        if ("READ_COMMITTED".equals(isolationLevel)) {
            this.isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
        } else if ("READ_UNCOMMITTED".equals(isolationLevel)) {
            this.isolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
        } else if ("REPEATABLE_READ".equals(isolationLevel)) {
            this.isolationLevel = Connection.TRANSACTION_REPEATABLE_READ;
        } else if ("SERIALIZABLE".equals(isolationLevel)) {
            this.isolationLevel = Connection.TRANSACTION_SERIALIZABLE;
        } else {
            throw new IllegalArgumentException(
                    "invalid isolation level. isolation level:" + isolationLevel);
        }
    }

    /**
     * 初期SQLを設定する。<br>
     * 本メソッドで設定されたSQLは、トランザクション開始時({@link JdbcTransaction#begin()})に実行される。
     *
     * @param initSqlList 初期SQLを保持するListオブジェクト
     */
    public void setInitSqlList(List<String> initSqlList) {
        this.initSqlList = initSqlList;
    }

    /**
     * トランザクションタイムアウト秒数設定を設定する。
     * <p/>
     * 設定を省略した場合または、0以下の値を設定した場合はトランザクションタイムアウト機能は無効化される。
     *
     * @param transactionTimeoutSec トランザクションタイムアウト秒数設定
     */
    public void setTransactionTimeoutSec(int transactionTimeoutSec) {
        this.transactionTimeoutSec = transactionTimeoutSec;
    }
}

