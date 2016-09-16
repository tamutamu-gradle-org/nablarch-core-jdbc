package nablarch.core.db.statement;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.transaction.JdbcTransactionTimeoutHandler;
import nablarch.core.util.annotation.Published;

/**
 * SQL文を実行するインタフェース。
 * <p/>
 * 本インタフェースでは、JDBC標準SQL(バインド変数が「?」)と拡張SQL(バインド変数が名前付き変数)で共通となるインタフェースを定義している。
 * <p/>
 * このクラスはリソースを解放する必要があるが、リソースの解放処理は{@link nablarch.core.db.connection.TransactionManagerConnection#terminate()}で行われるため、
 * Statementを明示的にクローズする必要はない。
 *
 * @author Hisaaki Sioiri
 */
public interface SqlStatement {

    /**
     * {@link java.sql.PreparedStatement#close}のラッパー。
     */
    void close();

    /**
     * Statementがクローズされているか否か。
     *
     * @return このStatementオブジェクトがクローズされている場合は {@code true}、まだオープンしている場合は {@code false}
     */
    boolean isClosed();

    /**
     * {@link java.sql.PreparedStatement#executeBatch}のラッパー。
     *
     * @return 更新件数
     */
    @Published
    int[] executeBatch();

    /**
     * バッチサイズを取得する。
     *
     * @return サイズ
     */
    @Published
    int getBatchSize();

    /**
     * トランザクションタイムアウトタイマーを設定する。
     * <p/>
     * 本設定値を省略した場合、トランザクションのタイムアウト処理は行われない。
     *
     * @param jdbcTransactionTimeoutHandler トランザクションタイムアウトタイマー
     */
    void setJdbcTransactionTimeoutHandler(JdbcTransactionTimeoutHandler jdbcTransactionTimeoutHandler);

    /**
     * Statementを生成した{@link AppDbConnection}を取得する。
     * 
     * @return データベース接続オブジェクト
     */
    AppDbConnection getConnection();

    /**
     * {@link java.sql.PreparedStatement#getFetchSize}のラッパー。
     *
     * @return フェッチする行数
     */
    int getFetchSize();

    /**
     * {@link java.sql.PreparedStatement#setFetchSize}のラッパー。
     *
     * @param rows フェッチする行数
     */
    void setFetchSize(int rows);

    /**
     * {@link java.sql.PreparedStatement#getUpdateCount}のラッパー。
     *
     * @return 更新件数
     */
    int getUpdateCount();

    /**
     * {@link java.sql.PreparedStatement#setQueryTimeout}のラッパー。
     *
     * @param seconds タイムアウト時間
     */
    void setQueryTimeout(int seconds);

    /**
     * {@link java.sql.PreparedStatement#getQueryTimeout}のラッパー。
     *
     * @return タイムアウト時間
     */
    int getQueryTimeout();

    /**
     * {@link java.sql.PreparedStatement#getMaxRows}のラッパー。
     *
     * @return この Statement オブジェクトによって生成される{@link java.sql.ResultSet}オブジェクトの現在の最大行数。ゼロは無制限を意味する。
     */
    int getMaxRows();

    /**
     * {@link java.sql.PreparedStatement#setMaxRows}のラッパー。
     *
     * @param max 新しい最大行数の制限値。ゼロは無制限を意味する。
     */
    void setMaxRows(int max);

    /**
     * {@link java.sql.PreparedStatement#clearBatch}のラッパー。
     */
    void clearBatch();
}

