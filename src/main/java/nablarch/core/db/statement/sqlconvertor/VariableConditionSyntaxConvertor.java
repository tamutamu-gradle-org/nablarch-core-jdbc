package nablarch.core.db.statement.sqlconvertor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nablarch.core.db.util.DbUtil;
import nablarch.core.util.StringUtil;

/**
 * SQL文の可変条件構文を変換するクラス。
 * 
 * @author Kiyohito Itoh
 */
public class VariableConditionSyntaxConvertor extends SqlConvertorSupport {
    
    /**
     * 可変条件を示す正規表現<br>
     * if(userName){user_name = :userName}を抜き出す正規表現
     */
    private static final Pattern VARIABLE_CONDITION = Pattern
            .compile("^\\$if[ ]*\\(([a-zA-Z0-9_]+)\\)[ ]*\\{[ ]*([^\\}]+)[ ]*\\}");

    /** if拡張構文を表す文字列 */
    private static final String IF_CONDITION = "$if";

    /** 配列(Collection)の要素1つでその中身が空文字列を許容するか否か */
    private boolean allowArrayEmptyString = true;

    /**
     * SQL文の可変条件構文を変換する。
     * <p/>
     * $if{}で囲われた条件部分を可変条件と見なし、対応する入力条件がnullまたは空文字列の場合には条件を評価しないようにする。<br>
     * <p/>
     * 例:
     * <pre>
     * 条件:where $if(userName){user_name = :userName}
     *      入力あり->where (0 = 1 or (user_name = :userName))
     *      入力なし->where (0 = 0 or (user_name = :userName))
     * </pre>
     * <p/>
     * パラメータで指定された、検索条件をもつオブジェクトの対象項目が、
     * 入力なし(nullまたは空文字列)の場合には、検索条件から除外する。<br>
     * ※その項目の型が、配列または{@link java.util.Collection}の場合には、
     * nullまたはサイズが0の場合に検索条件から除外する。<br>
     * 検索条件からの除外の方法は、該当の条件が評価されないようにダミーの条件を付加する。<br>
     * 例えば、user_name = :userNameという条件の場合。<br>
     * 入力ありの場合:(0 = 1 or (user_name = :userName)) // 0 = 1は、一致しないため、user_name = :userNameが評価される。<br>
     * 入力なしの場合:(0 = 0 or (user_name = :userName)) // 0 = 0は、一致するため、user_name = :userNameは評価されない。<br>
     * <p/>
     * 例:
     * <pre>
     * Entity entity = new Entity();
     * entity.setUserName("ユーザ名");
     * entity.setTel("");
     *
     * parser.parse(
     *      "SELECT "
     *        + "USER_ID "
     *    + "FROM "
     *        + "USER_MTR "
     *    + "WHERE "
     *        + "$if(userName) {USER_NAME = :userName%} "
     *        + "AND $if(tel) {TEL = :%tel%} "
     *        + "AND SKJ_FLG = '0'");
     *
     * parser.getSql(); ->
     *      SELECT USER_ID
     *        FROM USER_MTR
     *       WHERE (0 = 1 OR (USER_NAME = ?))
     *         AND (0 = 0 OR (TEL = ?))
     *         AND SKJ_FLG = '0'
     * parser.getNameList(); -> [userName%, %tel%]
     * </pre>
     *
     * @param sql SQL文
     * @param obj 検索条件をもつオブジェクト
     * @return 可変条件構文を変換したSQL文
     */
    public String convert(String sql, Object obj) {
        
        int conditionStart = sql.indexOf(IF_CONDITION);
        if (conditionStart == -1) {
            return sql;
        }
        
        int sqlLength = sql.length();
        Matcher matcher = VARIABLE_CONDITION.matcher(sql);
        matcher.region(conditionStart, sqlLength);
        
        StringBuilder sb = new StringBuilder(sqlLength);
        int start = 0;
        while (matcher.find()) {
            // 可変条件の文字列から、入力チェックを行うフィールド名を取得する。
            String parameterName = matcher.group(1);

            // 可変条件までのSQL文を構築
            sb.append(sql.substring(start, matcher.start()));

            // 未入力の場合は、評価不要な条件なので「0 = 0 or」を条件として挿入する。
            // フィールドの値がnull以外かつ、長さが0以外の場合は、
            // 対応する条件を評価する必要があるため「0 = 1 or」を条件として挿入する。
            Object value = getBindValue(obj, parameterName);
            if (!DbUtil.isArrayObject(value)) {
                // 配列以外の場合は、Stringに変換してチェックする。
                sb.append(
                        StringUtil.isNullOrEmpty(value.toString()) ? "(0 = 0 or (" : "(0 = 1 or (");
            } else {
                // 配列の場合
                int size = DbUtil.getArraySize(value);
                if (size == 0) {
                    sb.append("(0 = 0 or (");
                } else if (size == 1 && !allowArrayEmptyString) {
                    Object o = DbUtil.getArrayValue(value, 0);
                    if (o == null || StringUtil.isNullOrEmpty(o.toString())) {
                        sb.append("(0 = 0 or (");
                    } else {
                        sb.append("(0 = 1 or (");
                    }
                } else {
                    sb.append("(0 = 1 or (");
                }
            }

            sb.append(matcher.group(2)).append("))");
            start = matcher.end();
            conditionStart = sql.indexOf(IF_CONDITION, start);
            matcher.region(conditionStart != -1 ? conditionStart : sqlLength, sqlLength);
        }
        sb.append(sql.substring(start));
        return sb.toString();
    }

    /**
     * 配列({@link java.util.Collection}を含む)のサイズが1の場合で、
     * その要素の値が空文字列の場合にその項目を検索条件に含めるか否かを設定する。
     *
     * @param allowArrayEmptyString 検索条件に含める場合はtrueを設定する。
     */
    public void setAllowArrayEmptyString(boolean allowArrayEmptyString) {
        this.allowArrayEmptyString = allowArrayEmptyString;
    }
}

