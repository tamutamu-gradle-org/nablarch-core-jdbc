package nablarch.core.db.statement;

import nablarch.core.util.annotation.Published;

/**
 * SQL文の変換を行うインタフェース。
 * @author Kiyohito Itoh
 */
@Published(tag = "architect")
public interface SqlConvertor {

    /**
     * SQL文の変換を行う。
     * @param sql SQL文
     * @param obj 検索条件をもつオブジェクト
     * @return 変換後のSQL文
     */
    String convert(String sql, Object obj);
}
