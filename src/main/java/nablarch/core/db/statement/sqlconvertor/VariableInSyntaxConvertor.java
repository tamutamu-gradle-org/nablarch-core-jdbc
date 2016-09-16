package nablarch.core.db.statement.sqlconvertor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nablarch.core.db.util.DbUtil;

/**
 * SQL文の可変IN構文を変換するクラス。
 *
 * @author Kiyohito Itoh
 */
public class VariableInSyntaxConvertor extends SqlConvertorSupport {

    /**
     * 可変IN構文を示す正規表現<br>
     * :kbn[]を抜き出す正規表現
     */
    private static final Pattern VARIABLE_IN_SYNTAX = Pattern.compile(":([a-zA-Z0-9_]+)\\[\\]");

    /**
     * SQL文の可変IN構文を変換する。
     * <p/>
     * IN句を検索条件にもつSQL文のIN句の項目数を付加する。<br/>
     * IN句を動的に生成する場合には、バインド変数名に「[]」を付加すること。<br/>
     * また、対応するオブジェクトのフィールドタイプは、配列またはCollectionとすること。<br/>
     * <p/>
     * 本機能は、検索条件のオブジェクトにMapインタフェースの実装クラスを指定することは出来ない。
     * Mapクラスは、値に対する型情報が存在しないため、IN句を構築する際に値が配列またはCollectionであることのチェックが確実に行えないためである。
     * これは、テスト時に確実に型チェックを行えないことを意味し(例えば値がnullの場合は、その型が何かは実行時にはわからない)、
     * 予期せぬ不具合の温床となるため本フレームワークでは敢えてMapの使用を制限している。
     * <p/>
     * 例えば、「userKbn」をIN句のパラメータとして指定する場合には、「USER_KBN IN (:userKbn[])」となる。
     * <p/>
     * 例:
     * <pre>
     * Entity entity = new Entity();
     * entity.userKbn = new String[] {"1", "2"};
     *
     * parser.parse(
     *      "SELECT "
     *        + "USER_ID "
     *    + "FROM "
     *        + "USER_MTR "
     *    + "WHERE "
     *        + "$if(userKbn) {USER_KBN IN (:userKbn[])} "
     *
     * parser.getSql(); ->
     *      SELECT USER_ID
     *        FROM USER_MTR
     *       WHERE (0 = 1 OR (USER_KBN IN (?, ?)))
     * parser.getNameList(); -> [userKbn[0], userKbn[1]]
     * </pre>
     *
     * @param sql SQL文
     * @param obj 検索条件をもつオブジェクト
     * @return 可変IN構文を変換したSQL文
     */
    public String convert(String sql, Object obj) {

        int sqlLength = sql.length();
        Matcher matcher = VARIABLE_IN_SYNTAX.matcher(sql);

        StringBuilder sb = new StringBuilder(sqlLength);
        int start = 0;
        while (matcher.find()) {

            // 可変IN構文までのSQL文を構築する。
            sb.append(sql.substring(start, matcher.start()));

            // INパラメータ部分を構築する。
            sb.append(makeInParameter(matcher.group(1), obj));

            start = matcher.end();
        }
        sb.append(sql.substring(start));
        return sb.toString();
    }

    /**
     * INパラメータ部分を構築する。<br>
     *
     * @param parameterName パラメータ名称
     * @param obj 条件フィールドを持つオブジェクト
     * @return INパラメータ
     */
    private String makeInParameter(String parameterName, Object obj) {
        StringBuilder sb = new StringBuilder();
        Object value = getBindValue(obj, parameterName);
        if (!DbUtil.isArrayObject(value)) {
            // フィールドのタイプが配列か、Collectionでない場合はエラー
            throw new IllegalArgumentException(String.format(
                    "object type in field is invalid. valid object type is Collection or Array."
                            + " field name = [%s].", parameterName));
        }
        int size = DbUtil.getArraySize(value);
        if (size == 0) {
            return ':' + parameterName + "[]";
        }
        for (int i = 0; i < size; i++) {
            if (i != 0) {
                sb.append(',');
            }
            sb.append(':');
            sb.append(parameterName);
            sb.append('[');
            sb.append(i);
            sb.append(']');
        }
        return sb.toString();
    }
}
