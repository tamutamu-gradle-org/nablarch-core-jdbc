package nablarch.core.db.connection;

import java.sql.Connection;

import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.db.dialect.Dialect;
import nablarch.core.db.statement.SqlStatement;
import nablarch.core.db.transaction.JdbcTransactionTimeoutHandler;
import nablarch.core.util.annotation.Published;

/**
 * データベースに対するトランザクション制御を行うインタフェース。<br>
 *
 * @author Hisaaki Sioiri
 * @see Connection
 */
@Published(tag = "architect")
public interface TransactionManagerConnection extends AppDbConnection {

    /** データベース接続オブジェクトの初期化処理を行う。 */
    void initialize();

    /** 現在のデータベース接続に対してcommitを実行する。 */
    void commit();

    /** 現在のデータベース接続に対してrollbackを実行する。 */
    void rollback();

    /**
     * データベース接続の終了処理を行う。<br>
     * 実装クラスでは、最低限{@link Connection#close()}を呼び出しリソースの開放処理を行う必要がある。
     *
     * @see Connection#close()
     */
    void terminate();

    /**
     * アイソレーションレベルを設定する。
     *
     * @param level アイソレーションレベル
     * @see Connection#setTransactionIsolation(int)
     */
    void setIsolationLevel(int level);

    /**
     * トランザクションタイムアウトハンドラを設定する。
     *
     * @param jdbcTransactionTimeoutHandler トランザクションタイムアウトハンドラ
     */
    void setJdbcTransactionTimeoutHandler(JdbcTransactionTimeoutHandler jdbcTransactionTimeoutHandler);

    /**
     * データベース接続オブジェクトを取得する。
     * @return データベース接続オブジェクト
     */
    Connection getConnection();

    /**
     * コネクションの{@link DefaultDialect}を取得する。
     *
     * @return SQL方言
     */
    Dialect getDialect();

    /**
     * 保持しているStatementを削除する。
     * @param statement 削除対象のステートメント
     */
    void removeStatement(SqlStatement statement);
}

