package nablarch.core.db.statement.sqlconvertor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL文の可変ORDER BY構文を変換するクラス。
 * 
 * @author Kiyohito Itoh
 */
public class VariableOrderBySyntaxConvertor extends SqlConvertorSupport {

    /**
     * SQL文から可変ORDER BY構文を抜き出す正規表現
     * <pre>
     * "$sort(sortId){(1 USER_ID)(2 KANJI_NAME)(3 KANA_NAME)(default USER_ID)}"を抜き出す。
     * グループ1: "sortId"
     * グループ2: "(1 USER_ID)(2 KANJI_NAME)(3 KANA_NAME)(default USER_ID)"
     * </pre>
     */
    private static final Pattern VARIABLE_ORDER_BY_SYNTAX
        = Pattern.compile("^\\$sort[ ]*\\(([a-zA-Z0-9_]+)\\)[ ]*\\{[ ]*([^\\}]+)[ ]*\\}");

    /**
     * 可変ORDER BY構文のケース部分からソートIDに該当するケースを抜き出す正規表現のフォーマット
     * <p/>
     * この正規表現は、"%s"の部分をソートIDに置き換えてから使用する。
     * <pre>
     * ケース部分を"(1 USER_ID)(2 KANJI_NAME)(3 KANA_NAME)(default USER_ID)"とする。
     * ソートIDが"2"の場合は、"(2 KANJI_NAME)"と"(default USER_ID)"を抜き出す。
     * "(2 KANJI_NAME)"のグループ1: "2"
     * "(2 KANJI_NAME)"のグループ2: "KANJI_NAME"
     * "(default USER_ID)"のグループ1: "default"
     * "(default USER_ID)"のグループ2: "USER_ID"
     * </pre>
     */
    private static final String ORDER_BY_CASE_FORMAT = "\\([ ]*(%s)[ ]+([^\\)]+)\\)";

    /** デフォルトのケースに使用するソートID */
    private static final String DEFAULT_CASE_SORT_ID = "default";
    
    /**
     * SQL文の可変ORDER BY構文を変換する。
     * <p/>
     * 可変ORDER BY構文の仕様は下記のとおり。
     * <pre>
     * $sort(フィールド名) {(ケース1)(ケース2)・・・(ケースn)}
     * 
     * ケース: ORDER BY句の切り替え候補を表す。
     *         候補を一意に識別するソートIDとORDER BY句に指定する文字列(以降はケース本体と称す)を記述する。
     *         どの候補にも一致しない場合に使用するデフォルトのケースには、ソートIDに"default"を指定する。
     * フィールド名: 検索条件オブジェクトからソートIDを取得する際に使用するフィールド名を表す。
     * </pre>
     * ケース部分の仕様は下記のとおり。
     * <ul>
     * <li>各ケースは、ソートIDとケース本体を半角丸括弧で囲んで表現する。</li>
     * <li>ソートIDとケース本体は、半角スペースで区切る。</li>
     * <li>ソートIDには半角スペースを使用不可とする。ケース本体には半角スペースを使用できる。</li>
     * <li>括弧開き以降で最初に登場する文字列をソートIDとする。</li>
     * <li>ソートID以降で括弧閉じまでの間をケース本体とする。</li>
     * <li>ソートIDおよびケース本体はトリミングする。</li>
     * </ul>
     * 
     * 検索条件オブジェクトからフィールド名で指定された値を取得し、
     * 取得した値が一致するケースのケース本体をORDER BY句としてSQL文に追加する。
     * 取得した値が一致するケースが存在しない、かつデフォルトのケースが存在する場合は、
     * デフォルトのケース本体をORDER BY句としてSQL文に追加する。
     * 取得した値が一致するケースが存在しない、かつデフォルトのケースも存在しない場合は、
     * 可変ORDER BY構文を削除しただけのSQL文を返す。
     * 
     * 下記に例を示す。
     * <pre>
     * デフォルトのケースを指定したSQL文の場合
     * 
     *     SELECT * FROM USER WHERE USER_ID = :userId
     *     $sort(sortId) {(1 USER_ID ASC) (2 USER_ID DESC) (3 USER_ID, KANJI_NAME ASC) (4 USER_ID, KANJI_NAME DESC) (default USER_ID ASC)}
     * 
     * sortId = 1:
     * 
     *     SELECT * FROM USER WHERE USER_ID = :userId ORDER BY USER_ID ASC
     * 
     * sortId = 4:
     * 
     *     SELECT * FROM USER WHERE USER_ID = :userId ORDER BY USER_ID, KANJI_NAME DESC
     * 
     * sortId = null:
     * 
     *     SELECT * FROM USER WHERE USER_ID = :userId ORDER BY USER_ID ASC
     * 
     * デフォルトのケースを指定しないSQL文の場合
     * 
     *     SELECT * FROM USER WHERE USER_ID = :userId
     *     $sort(sortId) {(1 USER_ID ASC) (2 USER_ID DESC) (3 USER_ID, KANJI_NAME ASC) (4 USER_ID, KANJI_NAME DESC)}
     * 
     * sortId = null:
     * 
     *     SELECT * FROM USER WHERE USER_ID = :userId
     * 
     * </pre>
     * 
     * @param sql SQL文
     * @param obj 検索条件をもつオブジェクト
     * @return 可変ORDER BY構文を変換したSQL文
     */
    @Override
    public String convert(String sql, Object obj) {

        int orderBySyntaxStart = sql.indexOf("$sort");
        if (orderBySyntaxStart == -1) {
            return sql;
        }
        
        int sqlLength = sql.length();
        Matcher orderByMatcher = VARIABLE_ORDER_BY_SYNTAX.matcher(sql);
        orderByMatcher.region(orderBySyntaxStart, sqlLength);
        
        StringBuilder sb = new StringBuilder(sqlLength);
        
        int start = 0;
        while (orderByMatcher.find()) {
            
            // 可変ORDER BY構文までのSQL文を追加する。
            sb.append(sql.substring(start, orderByMatcher.start())); 
            
            // ソートIDを取得する。
            String propName = orderByMatcher.group(1); 
            Object sortIdObj = getBindValue(obj, propName);
            
            // ケースを取得する。
            String cases = orderByMatcher.group(2);
            
            String orderBy = null;
            
            // ソートIDが取得できた場合は、ソートIDのケースを取得する。
            if (sortIdObj != null) {
                orderBy = getCase(cases, sortIdObj.toString());
            }
            
            // ソートIDのケースが取得できない場合は、デフォルトのケースを取得する。
            if (orderBy == null) {
                orderBy = getCase(cases, DEFAULT_CASE_SORT_ID);
            }
            
            // ORDER BY句が取得できた場合は追加する。
            if (orderBy != null) {
                sb.append("ORDER BY " + orderBy.trim());
            }
            
            start = orderByMatcher.end();
            orderBySyntaxStart = sql.indexOf("$sort", start);
            orderByMatcher.region(orderBySyntaxStart != -1 ? orderBySyntaxStart : sqlLength, sqlLength);
        }
        sb.append(sql.substring(start));
        
        return sb.toString();
    }
    
    /**
     * 可変ORDER BY構文のケース部分から指定されたソートIDに該当するケースを抜き出す。
     * @param cases 可変ORDER BY構文のケース部分
     * @param sortId ソートID
     * @return ケース。該当するケースが見つからない場合はnull
     */
    private String getCase(String cases, String sortId) {
        String useCasePattern = String.format(ORDER_BY_CASE_FORMAT, sortId);
        Pattern casePattern = Pattern.compile(useCasePattern);
        Matcher caseMatcher = casePattern.matcher(cases);
        return caseMatcher.find() ? caseMatcher.group(2) : null;
    }
}
