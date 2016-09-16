package nablarch.core.db.statement;

import java.util.List;

import nablarch.core.util.annotation.Published;

/**
 * 名前付きバインド変数を持つSQL文を解析するインタフェース。<br>
 * 名前付きバインド変数を持つSQL文を解析し、JDBC標準のSQL文(バインド変数を「?」に置き換えたSQL)と名前付きバインド変数のListを生成する。<br>
 * 例:<br>
 * <pre>
 * 通常のSQLの場合
 * parser.parse("insert into user_mst (user_id, name, create_user_id, :upd_user_id) values (:userId, :name, :updUserId, :updUserId");
 * parser.getSql(); ->  insert into user_mst (user_id, name, create_user_id, :upd_user_id) values (?, ?, ?, ?)
 * parser.getNameList() -> [userId, name, updUserId, updUserId]
 * <p/>
 * like句をもつSQLの場合
 * parser.setLikeEscapeChar('\\');
 * parser.parse("select user_name from user_mtr where user_id = :userId% and user_name = :%userName%");
 * parser.getSql(); ->  select user_name from user_mtr where user_id = ? escape '\' and user_name = ? escape '\'
 * parser.getNameList() -> [userId%, %userName%]
 * </pre>
 *
 * @author Hisaaki Sioiri
 */
@Published(tag = "architect")
public interface SqlParameterParser {

    /**
     * 名前付きバインド変数をもつSQL文を解析する。
     *
     * @param sql SQL文
     */
    void parse(String sql);

    /**
     * Nablarchの拡張構文が埋め込まれたSQL文をJDBC標準のSQL文(バインド変数を「?」に置き換えたSQL)に変換する。
     * @param sql SQL文
     * @param obj 検索条件をもつオブジェクト
     * @return 変換後のSQL文
     */
    String convertToJdbcSql(String sql, Object obj);

    /**
     * SQL文を取得する。<br>
     *
     * @return SQL文
     *         (名前付きバインド変数を「?」に置き換えたSQL文)
     */
    String getSql();

    /**
     * 名前付きバインド変数のListを取得する。
     *
     * @return 名前付きバインド変数のリスト
     */
    List<String> getNameList();

    /**
     * like条件のエスケープ対象文字をエスケープする文字を設定する。<br>
     * 本メソッドで設定されたSQL文は、解析後のSQL文のescape句に埋め込まれる。
     *
     * @param likeEscapeChar エスケープ文字
     */
    void setLikeEscapeChar(char likeEscapeChar);

}
