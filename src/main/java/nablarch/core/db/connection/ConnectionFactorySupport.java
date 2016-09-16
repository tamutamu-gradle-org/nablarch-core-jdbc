package nablarch.core.db.connection;

import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.db.dialect.Dialect;
import nablarch.core.db.statement.StatementFactory;
import nablarch.core.util.annotation.Published;

/**
 * {@link ConnectionFactory}の実装をサポートするクラス。
 * <p/>
 * 本クラスは、実装クラスで必要となる{@link nablarch.core.db.statement.StatementFactory}とStatementキャッシュの設定値をもつ。
 *
 * @author Hisaaki Sioiri
 */
@Published(tag = "architect")
public abstract class ConnectionFactorySupport implements ConnectionFactory {

    /** Statementファクトリオブジェクト */
    protected StatementFactory statementFactory; // SUPPRESS CHECKSTYLE サブクラスで使用するフィールドのため。

    /** Statementのキャッシュ有無(デフォルトは、キャッシュ無) */
    protected boolean statementReuse = true; // SUPPRESS CHECKSTYLE サブクラスで使用するフィールドのため。

    /** {@link nablarch.core.db.DbAccessException}ファクトリオブジェクト */
    protected DbAccessExceptionFactory dbAccessExceptionFactory; // SUPPRESS CHECKSTYLE サブクラスで使用するフィールドのため。

    /** SQL方言 */
    protected Dialect dialect = new DefaultDialect();  // SUPPRESS CHECKSTYLE サブクラスで使用するフィールドのため。

    /**
     * {@link StatementFactory}実装クラスを設定する。<br>
     *
     * @param statementFactory ステートメントファクトリオブジェクト
     * @see nablarch.core.db.statement.StatementFactory
     */
    public void setStatementFactory(StatementFactory statementFactory) {
        this.statementFactory = statementFactory;
    }

    /**
     * ステートメントのキャッシュ有無を設定する。<br>
     *
     * @param statementReuse ステートメントのキャッシュ有無
     */
    public void setStatementReuse(boolean statementReuse) {
        this.statementReuse = statementReuse;
    }

    /**
     * {@link nablarch.core.db.DbAccessException}ファクトリオブジェクトを設定する。
     * @param dbAccessExceptionFactory {@link nablarch.core.db.DbAccessException}ファクトリオブジェクト
     */
    public void setDbAccessExceptionFactory(DbAccessExceptionFactory dbAccessExceptionFactory) {
        this.dbAccessExceptionFactory = dbAccessExceptionFactory;
    }

    /**
     * SQL方言を設定する。
     *
     * @param dialect SQL方言
     */
    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    /**
     * データベース接続オブジェクトの初期化を行う。
     * <p/>
     * 下記の処理を行う。
     * <ul>
     * <li>BasicDbConnection#initialize()を呼び出し初期化を行う。</li>
     * <li>Statement生成用Factoryを設定する。</li>
     * <li>ステートメントのキャッシュ有無を設定する。</li>
     * <li>{@link nablarch.core.db.DbAccessException}ファクトリオブジェクトを設定する。</li>
     * </ul>
     * @param dbConnection データベース接続オブジェクト
     * @param connectionName 接続名
     */
    protected void initConnection(BasicDbConnection dbConnection, String connectionName) {
        dbConnection.initialize();
        dbConnection.setFactory(statementFactory);
        dbConnection.setStatementReuse(statementReuse);
        dbConnection.setDbAccessExceptionFactory(dbAccessExceptionFactory);
        setContext(dbConnection, connectionName);
    }

    /**
     * コンテキストを設定する。
     *
     * @param dbConnection データベース接続オブジェクト
     * @param connectionName 接続名
     */
    protected void setContext(BasicDbConnection dbConnection, String connectionName) {
        DbExecutionContext context = new DbExecutionContext(dbConnection, this.dialect, connectionName);
        dbConnection.setContext(context);
    }
}
