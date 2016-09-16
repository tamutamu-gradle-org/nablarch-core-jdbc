package nablarch.core.db.statement;

import nablarch.core.util.annotation.Published;

/**
 * {@link SqlParameterParser}を生成するインタフェース。
 *
 * @author Hisaaki Sioiri
 */
@Published(tag = "architect")
public interface SqlParameterParserFactory {

    /**
     * SqlParameterParserのインスタンスを生成し返却する。
     *
     * @return SqlParameterParser
     */
    SqlParameterParser createSqlParameterParser();

}
