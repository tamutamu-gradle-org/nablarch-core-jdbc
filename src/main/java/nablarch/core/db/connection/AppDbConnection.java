package nablarch.core.db.connection;

import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.SelectOption;
//import nablarch.core.db.statement.SqlCStatement;
import nablarch.core.db.statement.SqlCStatement;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.util.annotation.Published;

/**
 * データベース接続を表すインタフェース。
 * <p/>
 * データベースにアクセスを行う場合には、本インタフェース経由でSQL文実行用のオブジェクトを取得する必要がある。
 *
 * @author Hisaaki Sioiri
 * @see DbConnectionContext#getConnection()
 * @see DbConnectionContext#getConnection(String)
 */
public interface AppDbConnection {

    /**
     * パラメータ付きSQL文実行用のStatementオブジェクトを生成する。
     * <p/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     *
     * @param sql SQL文
     * @return {@link java.sql.PreparedStatement}のラッパーstatement
     * @see java.sql.Connection#prepareStatement(String)
     */
    @Published(tag = "architect")
    SqlPStatement prepareStatement(String sql);

    /**
     * 検索範囲を指定したパラメータ付きSQL文実行用のStatementオブジェクトを生成する。
     * <p/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     * <p/>
     * 注意:本メソッドで返却される{@link SqlPStatement}は、あらかじめ検索範囲が設定されているため、
     * {@link SqlPStatement#retrieve(int, int)}のような、検索範囲を指定した簡易検索処理は実行できない。<br />
     * {@link SqlPStatement#retrieve()}を使用すること。
     *
     * @param sql SQL文
     * @param selectOption 検索処理オプション
     * @return {@link java.sql.PreparedStatement}のラッパーstatement
     * @see java.sql.Connection#prepareStatement(String)
     */
    @Published(tag = "architect")
    SqlPStatement prepareStatement(String sql, SelectOption selectOption);

    /**
     * 自動生成キー（データベース側で自動生成された値）を取得する機能を持つStatementオブジェクトを生成する。
     * <p/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     * <p/>
     * 自動生成されたキーは、{@link nablarch.core.db.statement.SqlPStatement#getGeneratedKeys()}を使用して取得する必要がある。
     *
     * @param sql SQL文
     * @param autoGeneratedKeys 自動生成キーを返すかどうかを示すフラグ。{@link java.sql.Statement#RETURN_GENERATED_KEYS} または {@link java.sql.Statement#NO_GENERATED_KEYS}
     * @return 自動生成キーの取得機能を持つ{@link java.sql.PreparedStatement}のラッパーStatement
     * @see java.sql.Connection#prepareStatement(String, int)
     */
    @Published(tag = "architect")
    SqlPStatement prepareStatement(String sql, int autoGeneratedKeys);

    /**
     * 自動生成キー（データベース側で自動生成された値）を取得する機能を持つStatementオブジェクトを生成する。
     * <p/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     * <p/>
     * 自動生成されたキーは、{@link nablarch.core.db.statement.SqlPStatement#getGeneratedKeys()}を使用して取得する必要がある。
     *
     * @param sql SQL文
     * @param columnIndexes 挿入された行から返される列を示す列インデックスの配列
     * @return 自動生成キーの取得機能を持つ{@link java.sql.PreparedStatement}のラッパーStatement
     * @see java.sql.Connection#prepareStatement(String, int[])
     */
    @Published(tag = "architect")
    SqlPStatement prepareStatement(String sql, int[] columnIndexes);

    /**
     * 自動生成キー（データベース側で自動生成された値）を取得する機能を持つStatementオブジェクトを生成する。
     * <p/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     * <p/>
     * 自動生成されたキーは、{@link nablarch.core.db.statement.SqlPStatement#getGeneratedKeys()}を使用して取得する必要がある。
     *
     * @param sql SQL文
     * @param columnNames 挿入された行から返される列を示す列名の配列
     * @return 自動生成キーの取得機能を持つ{@link java.sql.PreparedStatement}のラッパーStatement
     * @see java.sql.Connection#prepareStatement(String, String[])
     */
    @Published(tag = "architect")
    SqlPStatement prepareStatement(String sql, String[] columnNames);

    /**
     * パラメータ付きSQL文実行用のStatementオブジェクトをSQL_IDを元に生成する。
     * <p/>
     * 指定されたSQL_IDに紐づくSQL文を取得し、Statementオブジェクトを生成する。<br/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     *
     * @param sqlId SQL_ID
     * @return {@link java.sql.PreparedStatement}のラッパーStatement
     * @see java.sql.Connection#prepareStatement(String)
     */
    @Published
    SqlPStatement prepareStatementBySqlId(String sqlId);

    /**
     * 検索範囲を指定したパラメータ付きSQL文実行用のStatementオブジェクトをSQL_IDを元に生成する。
     * <p/>
     * 指定されたSQL_IDに紐づくSQL文を取得し、Statementオブジェクトを生成する。<br/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     * <p/>
     * 注意:本メソッドで返却される{@link SqlPStatement}は、あらかじめ検索範囲が設定されているため、
     * {@link SqlPStatement#retrieve(int, int)}のような、検索範囲を指定した簡易検索処理は実行できない。<br />
     * {@link SqlPStatement#retrieve()}を使用すること。
     *
     * @param sqlId SQL_ID
     * @param selectOption 検索処理オプション
     * @return {@link java.sql.PreparedStatement}のラッパーstatement
     * @see java.sql.Connection#prepareStatement(String)
     */
    @Published(tag = "architect")
    SqlPStatement prepareStatementBySqlId(String sqlId, SelectOption selectOption);

    /**
     * 名前付きパラメータをもつSQL文実行用のStatementオブジェクトを生成する。
     * <p/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     * <p/>
     * 注意:本メソッドで返却される{@link ParameterizedSqlPStatement}は、名前付きパラメータをもつSQL文専用である。
     * このため、通常のバインド変数(バインド変数を「?」で表すもの)をもつSQL文の場合は、
     * 本メソッドで生成した{@link ParameterizedSqlPStatement}では処理できないことに注意すること。
     *
     * @param sql SQL文
     * @return Statementオブジェクト
     * @see java.sql.Connection#prepareStatement(String)
     */
    @Published(tag = "architect")
    ParameterizedSqlPStatement prepareParameterizedSqlStatement(String sql);

    /**
     * 検索範囲を指定した名前付きパラメータをもつSQL文実行用のStatementオブジェクトを生成する。
     * <p/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     * <p/>
     * 注意:本メソッドで返却される{@link ParameterizedSqlPStatement}は、名前付きパラメータをもつSQL文専用である。
     * このため、通常のバインド変数(バインド変数を「?」で表すもの)をもつSQL文の場合は、
     * 本メソッドで生成した{@link ParameterizedSqlPStatement}では処理できないことに注意すること。
     * <p/>
     * 注意:本メソッドで返却される{@link ParameterizedSqlPStatement}は、あらかじめ検索範囲が設定されているため、
     * {@link ParameterizedSqlPStatement#retrieve(int, int, Object)}のような、検索範囲を指定した簡易検索処理は実行できない。
     *
     * @param sql SQL文
     * @param selectOption 検索処理オプション
     * @return Statementオブジェクト
     * @see java.sql.Connection#prepareStatement(String)
     */
    @Published(tag = "architect")
    ParameterizedSqlPStatement prepareParameterizedSqlStatement(String sql, SelectOption selectOption);

    /**
     * 名前付きパラメータをもつSQL文実行用のStatementオブジェクトをSQL_IDを元に生成する。
     * <p/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     * <p/>
     * 注意:本メソッドで返却される{@link ParameterizedSqlPStatement}は、名前付きパラメータをもつSQL文専用である。
     * このため、通常のバインド変数(バインド変数を「?」で表すもの)をもつSQL文の場合は、
     * 本メソッドで生成した{@link ParameterizedSqlPStatement}では処理できないことに注意すること。
     *
     * @param sqlId SQL_ID
     * @return Statementオブジェクト
     * @see java.sql.Connection#prepareStatement(String)
     */
    @Published
    ParameterizedSqlPStatement prepareParameterizedSqlStatementBySqlId(String sqlId);

    /**
     * 検索範囲を設定した名前付きパラメータをもつSQL文実行用のStatementオブジェクトをSQL_IDを元に生成する。
     * <p/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     * <p/>
     * 注意:本メソッドで返却される{@link ParameterizedSqlPStatement}は、名前付きパラメータをもつSQL文専用である。
     * このため、通常のバインド変数(バインド変数を「?」で表すもの)をもつSQL文の場合は、
     * 本メソッドで生成した{@link ParameterizedSqlPStatement}では処理できないことに注意すること。
     * <p/>
     * 注意:本メソッドで返却される{@link ParameterizedSqlPStatement}は、あらかじめ検索範囲が設定されているため、
     * {@link ParameterizedSqlPStatement#retrieve(int, int, Object)}のような、検索範囲を指定した簡易検索処理は実行できない。
     *
     * @param sqlId SQL_ID
     * @param selectOption 検索処理オプション
     * @return Statementオブジェクト
     * @see java.sql.Connection#prepareStatement(String)
     */
    @Published(tag = "architect")
    ParameterizedSqlPStatement prepareParameterizedSqlStatementBySqlId(String sqlId, SelectOption selectOption);

    /**
     * 名前付きパラメータをもつ可変条件SQL文実行用のStatementオブジェクトを生成する。
     * <p/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     * <p/>
     * 注意:本メソッドで返却される{@link ParameterizedSqlPStatement}は、名前付きパラメータをもつSQL文専用である。
     * このため、通常のバインド変数(バインド変数を「?」で表すもの)をもつSQL文の場合は、
     * 本メソッドで生成した{@link ParameterizedSqlPStatement}では処理できないことに注意すること。
     *
     * @param sql SQL文
     * @param condition 可変条件に設定される条件をもつオブジェクト
     * @return Statementオブジェクト
     * @see java.sql.Connection#prepareStatement(String)
     */
    @Published(tag = "architect")
    ParameterizedSqlPStatement prepareParameterizedSqlStatement(String sql, Object condition);

    /**
     * 検索範囲を指定して、名前付きパラメータをもつ可変条件SQL文実行用のStatementオブジェクトを生成する。
     * <p/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     * <p/>
     * 注意:本メソッドで返却される{@link ParameterizedSqlPStatement}は、名前付きパラメータをもつSQL文専用である。
     * このため、通常のバインド変数(バインド変数を「?」で表すもの)をもつSQL文の場合は、
     * 本メソッドで生成した{@link ParameterizedSqlPStatement}では処理できないことに注意すること。
     * <p/>
     * 注意:本メソッドで返却される{@link ParameterizedSqlPStatement}は、あらかじめ検索範囲が設定されているため、
     * {@link ParameterizedSqlPStatement#retrieve(int, int, Object)}のような、検索範囲を指定した簡易検索処理は実行できない。
     *
     * @param sql SQL文
     * @param condition 可変条件に設定される条件をもつオブジェクト
     * @param selectOption 検索処理オプション
     * @return Statementオブジェクト
     * @see java.sql.Connection#prepareStatement(String)
     */
    @Published(tag = "architect")
    ParameterizedSqlPStatement prepareParameterizedSqlStatement(String sql, Object condition, SelectOption selectOption);

    /**
     * 名前付きパラメータをもつ可変条件SQL文実行用のStatementオブジェクトをSQL_IDを元に生成する。
     * <p/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     * <p/>
     * 注意:本メソッドで返却される{@link ParameterizedSqlPStatement}は、名前付きパラメータをもつSQL文専用である。
     * このため、通常のバインド変数(バインド変数を「?」で表すもの)をもつSQL文の場合は、
     * 本メソッドで生成した{@link ParameterizedSqlPStatement}では処理できないことに注意すること。
     *
     * @param sqlId SQL_ID
     * @param condition 可変条件に設定される条件をもつオブジェクト
     * @return Statementオブジェクト
     * @see java.sql.Connection#prepareStatement(String)
     */
    @Published
    ParameterizedSqlPStatement prepareParameterizedSqlStatementBySqlId(String sqlId, Object condition);

    /**
     * 検索範囲を設定した名前付きパラメータをもつ可変条件SQL文実行用のStatementオブジェクトをSQL_IDを元に生成する。
     * <p/>
     * Statementオブジェクトは、{@link TransactionManagerConnection#terminate()}メソッドで自動的にクローズされるため、
     * アプリケーションは、取得したStatementオブジェクトを明示的にクローズする必要はない。
     * <p/>
     * 注意:本メソッドで返却される{@link ParameterizedSqlPStatement}は、名前付きパラメータをもつSQL文専用である。
     * このため、通常のバインド変数(バインド変数を「?」で表すもの)をもつSQL文の場合は、
     * 本メソッドで生成した{@link ParameterizedSqlPStatement}では処理できないことに注意すること。
     * <p/>
     * 注意:本メソッドで返却される{@link ParameterizedSqlPStatement}は、あらかじめ検索範囲が設定されているため、
     * {@link ParameterizedSqlPStatement#retrieve(int, int, Object)}のような、検索範囲を指定した簡易検索処理は実行できない。
     *
     * @param sqlId SQL_ID
     * @param condition 可変条件に設定される条件をもつオブジェクト
     * @param selectOption 検索処理オプション
     * @return Statementオブジェクト
     * @see java.sql.Connection#prepareStatement(String)
     */
    @Published(tag = "architect")
    ParameterizedSqlPStatement prepareParameterizedSqlStatementBySqlId(String sqlId, Object condition, SelectOption selectOption);

    /**
     * SQL_IDを元に件数取得(カウント)用のStatementオブジェクトを生成する。
     * <p/>
     * SQL文を件数取得(カウント)用に変換すること以外は、{@link #prepareParameterizedSqlStatementBySqlId(String, Object)}と同じ処理を行う。
     *
     * @param sqlId SQL_ID
     * @param condition 可変条件に設定される条件をもつオブジェクト
     * @return Statementオブジェクト
     * @see java.sql.Connection#prepareStatement(String)
     */
    @Published
    ParameterizedSqlPStatement prepareParameterizedCountSqlStatementBySqlId(String sqlId, Object condition);

    /**
     * SQL_IDを元に件数取得（カウント）用のStatementオブジェクトを生成する。
     * <p/>
     * SQL文を件数取得（カウント）用に変換すること以外は、{@link #prepareStatementBySqlId(String)}と同じ処理を行う。
     *
     * @param sqlId SQL_ID
     * @return Statementオブジェクト
     * @see java.sql.Connection#prepareStatement(String)
     */
    @Published
    SqlPStatement prepareCountStatementBySqlId(String sqlId);

    /**
     * ストアドプロシージャ実行用のStatementオブジェクトを生成する。
     *
     * @param sql SQL文
     * @return Statementオブジェクト
     * @see java.sql.Connection#prepareCall(String)
     */
    @Published(tag = "architect")
    SqlCStatement prepareCall(String sql);

    /**
     * ストアドプロシージャ実行用のStatementオブジェクトをSQL_IDを元に生成する。
     *
     * @param sqlId SQL_ID
     * @return Statementオブジェクト
     * @see java.sql.Connection#prepareCall(String)
     */
    @Published
    SqlCStatement prepareCallBySqlId(String sqlId);
}

