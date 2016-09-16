package nablarch.core.db.transaction;

import java.util.List;

import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.transaction.Transaction;


/**
 * データベースに対してJDBCレベルでトランザクション制御を行うクラス。
 *
 * @author Hisaaki Sioiri
 * @see TransactionManagerConnection
 */
public class JdbcTransaction implements Transaction {

    /** SQLログを出力するロガー */
    private static final Logger SQL_LOGGER = LoggerManager.get("SQL");

    /** コネクション名 */
    private String connectionName;

    /** アイソレーションレベル */
    private int isolationLevel;

    /** 初期SQL */
    private List<String> initSqlList;

    /** トランザクションタイムアウト設定 */
    private JdbcTransactionTimeoutHandler transactionTimeoutHandler;

    /**
     * 指定されたトランザクション名に対するトランザクションオブジェクトを生成する。
     *
     * @param connectionName コネクション名
     */
    public JdbcTransaction(String connectionName) {
        this.connectionName = connectionName;
    }

    /**
     * トランザクションを開始する。<br>
     * トランザクション開始時には、下記の処理を行う。<br>
     * <ol>
     * <li>ロールバックを行う。</li>
     * <li>アイソレーションレベルを設定する。</li>
     * <li>初期SQLのリストをすべて実行し、コミットを行う。</li>
     * <li>トランザクションタイムアウトの監視を開始する。（トランザクションタイムアウト設定が行われている場合のみ）</li>
     * </ol>
     * JDBC経由のトランザクション制御では、トランザクションの開始を明示的に行えないため、
     * ロールバックを行い、未コミット情報のないクリアなトランザクションを生成する。
     */
    public void begin() {
        TransactionManagerConnection con = (TransactionManagerConnection) DbConnectionContext.getConnection(
                connectionName);
        con.rollback();
        con.setIsolationLevel(isolationLevel);
        executeInitSql(con);
        beginMonitorTransactionTimeout(con);
    }

    /**
     * 初期SQLを実行しコミット処理を行う。
     *
     * 初期SQLのリストが空の場合には、何も行わない。
     * @param connection トランザクション管理用コネクション
     */
    private void executeInitSql(TransactionManagerConnection connection) {
        if (initSqlList.isEmpty()) {
            return;
        }
        for (String initSql : initSqlList) {
            SqlPStatement statement = connection.prepareStatement(initSql);
            statement.execute();
            statement.close();
        }
        connection.commit();
    }

    /**
     * トランザクションタイムアウトの監視を開始する。
     *
     * @param connection トランザクション管理用コネクション
     */
    private void beginMonitorTransactionTimeout(TransactionManagerConnection connection) {
        if (transactionTimeoutHandler == null) {
            return;
        }
        transactionTimeoutHandler.begin();
        connection.setJdbcTransactionTimeoutHandler(transactionTimeoutHandler);
    }

    /** {@inheritDoc} */
    public void commit() {
        TransactionManagerConnection con = (TransactionManagerConnection) DbConnectionContext.getConnection(
                connectionName);
        con.commit();
        if (SQL_LOGGER.isDebugEnabled()) {
            SQL_LOGGER.logDebug("transaction commit. resource=[" + connectionName + ']');
        }
    }

    /** {@inheritDoc} */
    public void rollback() {
        TransactionManagerConnection con = (TransactionManagerConnection) DbConnectionContext.getConnection(
                connectionName);
        con.rollback();
        if (SQL_LOGGER.isDebugEnabled()) {
            SQL_LOGGER.logDebug("transaction rollback. resource=[" + connectionName + ']');
        }
    }

    /**
     * アイソレーションレベルを設定する。
     *
     * @param isolationLevel アイソレーションレベル
     */
    void setIsolationLevel(int isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    /**
     * 初期SQLを設定する。<br>
     * 本メソッドで設定されたSQLは、トランザクション開始時に一括して実行される。
     *
     * @param initSqlList 初期SQLをもつList
     */
    void setInitSqlList(List<String> initSqlList) {
        this.initSqlList = initSqlList;
    }

    /**
     * トランザクションタイムアウトハンドラを設定する。
     *
     * @param transactionTimeoutHandler トランザクションタイムアウトハンドラ
     */
    public void setTransactionTimeoutHandler(JdbcTransactionTimeoutHandler transactionTimeoutHandler) {
        this.transactionTimeoutHandler = transactionTimeoutHandler;
    }
}

