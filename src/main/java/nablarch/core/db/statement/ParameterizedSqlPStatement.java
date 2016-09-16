package nablarch.core.db.statement;

import java.util.Map;

import nablarch.core.db.statement.exception.SqlStatementException;
import nablarch.core.util.annotation.Published;


/**
 * 名前付きバインド変数をもつSQL文を実行するインタフェース。
 *
 * @author Hisaaki Sioiri
 * @see java.sql.PreparedStatement
 */
@Published
public interface ParameterizedSqlPStatement extends SqlStatement {

    /**
     * 簡易検索機能。
     * <p/>
     * 下記設定で検索を実行する。
     * <ul>
     *     <li>読み込み開始位置 = 1</li>
     *     <li>最大行数 = 無制限</li>
     * </ul>
     * 本メソッドを使用すると{@link #setMaxRows}で事前に設定した値は適用されない。
     *
     * @param data 検索条件を要素にもつMap
     * @return 取得結果
     * @throws SqlStatementException SQL実行時に{@link java.sql.SQLException}が発生した場合
     */
    SqlResultSet retrieve(Map<String, ?> data) throws SqlStatementException;

    /**
     * 簡易検索機能。
     *
     * @param startPos 取得開始位置
     * @param max      取得最大件数
     * @param data     検索条件を要素にもつMap
     * @return 取得結果
     * @throws SqlStatementException SQL実行時に{@link java.sql.SQLException}が発生した場合
     */
    SqlResultSet retrieve(int startPos, int max, Map<String, ?> data) throws SqlStatementException;

    /**
     * 簡易検索機能。
     * <p/>
     * 下記設定で検索を実行する。
     * <ul>
     *     <li>読み込み開始位置 = 1</li>
     *     <li>最大行数 = 無制限</li>
     * </ul>
     * 本メソッドを使用すると{@link #setMaxRows}で事前に設定した値は適用されない。
     *
     * @param data 検索条件をフィールドにもつオブジェクト
     * @return 取得結果
     * @throws SqlStatementException SQL実行時に{@link java.sql.SQLException}が発生した場合
     */
    SqlResultSet retrieve(Object data) throws SqlStatementException;

    /**
     * 簡易検索機能。
     *
     * @param startPos 取得開始位置
     * @param max      取得最大件数
     * @param data     検索条件をフィールドにもつオブジェクト
     * @return 取得結果
     * @throws SqlStatementException SQL実行時に{@link java.sql.SQLException}が発生した場合
     */
    SqlResultSet retrieve(int startPos, int max, Object data) throws SqlStatementException;

    /**
     * {@link java.sql.PreparedStatement#executeQuery}のラッパー。
     *
     * @param data 検索条件を要素にもつMap
     * @return 取得結果
     * @throws SqlStatementException SQL実行時に{@link java.sql.SQLException}が発生した場合
     */
    ResultSetIterator executeQueryByMap(Map<String, ?> data) throws SqlStatementException;

    /**
     * {@link java.sql.PreparedStatement#executeQuery}のラッパー。
     *
     * @param data 検索条件をフィールドの値にもつオブジェクト
     * @return 取得結果
     * @throws SqlStatementException SQL実行時に{@link java.sql.SQLException}が発生した場合
     */
    ResultSetIterator executeQueryByObject(Object data) throws SqlStatementException;

    /**
     * オブジェクトのフィールドの値をバインド変数に設定しSQLを実行する。
     *
     * @param data バインド変数にセットする値を保持したオブジェクト
     * @return 更新件数
     * @throws SqlStatementException 例外発生時
     */
    int executeUpdateByObject(Object data) throws SqlStatementException;

    /**
     * バッチ実行用にオブジェクトのフィールドの値をバインド変数にセットする。
     *
     * @param data バインド変数にセットする値を保持したオブジェクト
     */
    void addBatchObject(Object data);

    /**
     * Mapのvalueをバインド変数にセットしSQLを実行する。
     *
     * @param data バインド変数にセットする値を保持したMap
     * @return 登録または、更新件数
     * @throws SqlStatementException 例外発生時
     */
    int executeUpdateByMap(Map<String, ?> data) throws SqlStatementException;

    /**
     * バッチ実行用にMapのvalueをバインド変数にセットする。
     *
     * @param data バインド変数にセットする値を保持したMap
     */
    void addBatchMap(Map<String, ?> data);
}
