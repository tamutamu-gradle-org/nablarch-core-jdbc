package nablarch.core.db.statement;

import java.util.ArrayList;
import java.util.List;

import nablarch.core.db.statement.sqlconvertor.VariableConditionSyntaxConvertor;
import nablarch.core.db.statement.sqlconvertor.VariableInSyntaxConvertor;
import nablarch.core.db.statement.sqlconvertor.VariableOrderBySyntaxConvertor;

/**
 * 名前付きバインド変数を持つSQL文を解析するクラス。<br>
 * SQLの解析は、下記ルールにしたがい行う。<br>
 * <ul>
 * <li>名前付き変数は「:」で開始される。</li>
 * <li>名前付き変数は、英字(大文字、小文字)、数字、アンダースコア(_)、パーセント(%)で構成されている。</li>
 * <li>リテラルは、シングルクォートで囲われている。</li>
 * <li>シングルクォートのエスケープは、シングルクォートである。</li>
 * <li>リテラル内に名前付き変数のルールに一致する文字列があっても、名前付き変数として扱わない。</li>
 * <li>コメントは存在しない。(コメント内に名前付き変数のルールに一致する文字列があった場合、名前付き変数として扱うためSQL実行時エラーが発生する。)</li>
 * <li>SQL文の妥当性チェックは行わない。(構文エラーがあった場合は、SQLの実行時エラーとなる。)</li>
 * </ul>
 * 本クラスは、解析したSQLの情報を保持するためスレッドアンセーフである。
 *
 * @author Hisaaki Sioiri
 */
public class BasicSqlParameterParser implements SqlParameterParser {

    /** 名前付き変数のリスト */
    private List<String> nameList;

    /** バインド変数を「?」に置き換えたSQL */
    private String sql;

    /** エスケープ文字 */
    private char likeEscapeChar;

    /** SqlConvertorのリスト */
    private SqlConvertor[] sqlConvertors = {
            new VariableConditionSyntaxConvertor(),
            new VariableInSyntaxConvertor(),
            new VariableOrderBySyntaxConvertor()
    };

    /**
     * SQL文を解析する。<br>
     *
     * @param sql 名前付きバインド変数を持つSQL文
     */
    public void parse(String sql) {
        // 可変条件の構築
        // 名前付きバインド変数の解析
        nameList = new ArrayList<String>();

        if (sql.contains(":")) {
            // コロンが存在する場合は、解析を行う。
            this.sql = replaceParam(sql);
        } else {
            // コロンがない場合(名前付き変数がない場合)は、
            // 解析を行う必要はないため実施しない。
            this.sql = sql;
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Nablarchの拡張構文の変換には、デフォルトで下記の{@link nablarch.core.db.statement.SqlConvertor}を使用する。
     * <ul>
     * <li>{@link nablarch.core.db.statement.sqlconvertor.VariableConditionSyntaxConvertor}</li>
     * <li>{@link nablarch.core.db.statement.sqlconvertor.VariableInSyntaxConvertor}</li>
     * <li>{@link nablarch.core.db.statement.sqlconvertor.VariableOrderBySyntaxConvertor}</li>
     * </ul>
     */
    public String convertToJdbcSql(String sql, Object obj) {
        String replaceSql = sql;
        for (SqlConvertor sqlConvertor : sqlConvertors) {
            replaceSql = sqlConvertor.convert(replaceSql, obj);
        }
        return replaceSql;
    }

    /**
     * SQL文を取得する。<br>
     * 返却されるSQL文は、名前付きバインド変数部を「?」に置き換えたSQL
     *
     * @return SQL文
     */
    public String getSql() {
        return sql;
    }

    /**
     * 名前付きバインド変数のリストを取得する。<br>
     * 名前付きバインド変数は、「?」の位置順にリストに格納されている。
     *
     * @return 名前付きバインド変数リスト
     */
    public List<String> getNameList() {
        return nameList;
    }


    /**
     * SQL文を解析し、名前付きバインド変数を「?」に置き換える。<br>
     *
     * 「?」への置き換え時に、バインド変数名に対応するバインド変数の位置(「?」の位置)を記憶する。<br/>
     *
     * パラメータのobjectがnull以外の場合は、名前付きバインド変数の解析は行わず、
     * 動的に条件式が変わるIN句の構築を行う。
     * IN句の構築例を以下に示す。<br/>
     * <code>
     * <pre>
     * // object
     * Entity entity = new Entity();
     * entity.setKbn = new String[] {"1", "2"};
     *
     * // SQL
     * SELECT USER_ID
     *   FROM USER_MTR
     *  WHERE KBN IN (:kbn[])
     *
     * // IN句構築後のSQL
     * // kbnに対応するobjectのフィールドの配列要素のサイズが2ため、
     * // IN句の条件式には2つの条件が設定される。
     * SELECT USER_ID
     *   FROM USER_MTR
     *  WHERE KBN IN (:kbn[0], :kbn[1])
     *
     * </pre>
     * </code>
     *
     *
     * @param sql SQL文
     * @return 変換後SQL
     */
    private String replaceParam(String sql) {
        int literalPos = sql.indexOf('\'');
        if (literalPos != -1) {
            // リテラルがある場合
            StringBuilder sb = new StringBuilder(sql.length());

            // リテラルの開始位置までを置き換え(再帰的に置き換え処理を呼び出す)
            sb.append(replaceParam(sql.substring(0, literalPos)));

            // リテラル内部は置き換え対象外
            // リテラルの終了位置までは、無条件に追加
            int end = sql.indexOf('\'', literalPos + 1);
            if (end == -1) {
                // リテラルが閉じられていない場合は、そのまま返却
                sb.append(sql.substring(literalPos));
                return sb.toString();
            }
            sb.append(sql.substring(literalPos, end + 1));

            // リテラル以降を置き換え(再帰的に置き換え処理を呼び出す)
            sb.append(replaceParam(sql.substring(end + 1)));
            return sb.toString();

        } else {
            // リテラルが存在しない場合
            StringBuilder sb = new StringBuilder(sql.length());
            int nameStart = sql.indexOf(':');
            int startPos = 0;
            while (nameStart != -1) {
                sb.append(sql.substring(startPos, nameStart));

                // パラメータ名を取得する。
                String paramName = getParamName(sql, ++nameStart);
                // 開始位置をパラメータ名の次の文字へシフト
                startPos = nameStart + paramName.length();
                // SQL文のパラメータ解析時
                // objectがnullの場合は、SQL文を解析しバインド変数名を「?」に変換し、
                // 変換前のバインド変数名を保持する。
                sb.append('?');

                StringBuilder inParam = new StringBuilder();
                if (sql.indexOf('[', startPos) == startPos) {
                    startPos = makeArrayParam(sql, startPos, inParam);
                } else {
                    if (paramName.startsWith("%") || paramName.endsWith("%")) {
                        // バインド変数名に「%」が存在している場合は、like検索と判断しエスケープ句を挿入する。
                        sb.append(" escape '").append(likeEscapeChar).append('\'');
                    }
                }
                nameList.add(paramName + inParam);
                nameStart = sql.indexOf(':', startPos);
            }
            sb.append(sql.substring(startPos));
            return sb.toString();
        }
    }

    /**
     * SQL文を解析し配列要素のパラメータ名を構築する。
     *
     * @param sql SQL文
     * @param startPos SQL文の解析開始位置
     * @param inParam 配列部分を構築するStringBuilder
     * @return 構築終了後の解析開始位置
     */
    private static int makeArrayParam(String sql, int startPos, StringBuilder inParam) {
        // 配列パラメータの場合
        boolean valid = false;
        while (startPos < sql.length()) {
            char c = sql.charAt(startPos++);
            if (c == '[' || c == ']' || ('0' <= c && c <= '9')) {
                inParam.append(c);
                if (c == ']') {
                    valid = true;
                    break;
                }
            } else {
                break;
            }
        }
        if (!valid) {
            throw new RuntimeException(
                    String.format("parameter name of Array is invalid. sql = [%s]", sql));
        }
        return startPos;
    }

    /**
     * 指定された開始位置から始まるパラメータ名を取得する。<br/>
     *
     * @param sql SQL文
     * @param start 開始位置
     * @return パラメータ名
     */
    private static String getParamName(String sql, int start) {
        int start1 = start;
        StringBuilder paramName = new StringBuilder();
        while (start1 < sql.length()) {
            char c = sql.charAt(start1++);
            if (('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9')
                    || (c == '%') || (c == '_')) {
                // パラメータ名は、a-z,A-Z,0-9,_,%のみで構成されていること。
                paramName.append(c);
            } else {
                break;
            }
        }
        return paramName.toString();
    }

    /**
     * エスケープ文字を設定する。<br>
     * SQL文のlike句に埋め込む条件のエスケープに使用する文字を設定する。
     * ここで設定した文字は、like条件のescape句に自動設定される。<br>
     * <code><pre>
     * String sql = "select user_id, user_kanji_name from usr where user_kanji_name like :%userKanjiName";
     * BasicSqlParameterParser parser = new BasicSqlParameterParser();
     * parser.setLikeEscapeChar("\");
     * parser.parse(sql);
     * parser.getSql(); -> select user_id, user_kanji_name from usr where user_kanji_name like ? ESCAPE '\'
     * </pre></code>
     *
     * @param likeEscapeChar エスケープ文字
     */
    public void setLikeEscapeChar(char likeEscapeChar) {
        this.likeEscapeChar = likeEscapeChar;
    }

    /**
     * SqlConvertorのリストを設定する。
     * 
     * @param sqlConvertors SqlConvertorのリスト
     */
    public void setSqlConvertors(List<SqlConvertor> sqlConvertors) {
        this.sqlConvertors = sqlConvertors.toArray(new SqlConvertor[sqlConvertors.size()]);
    }
}

