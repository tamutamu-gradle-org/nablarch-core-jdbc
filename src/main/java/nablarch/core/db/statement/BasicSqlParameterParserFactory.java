package nablarch.core.db.statement;

import java.util.List;

/**
 * {@link nablarch.core.db.statement.BasicSqlParameterParser}を生成する{@link nablarch.core.db.statement.SqlParameterParser}の基本実装クラス。
 *
 * @author Hisaaki Sioiri
 */
public class BasicSqlParameterParserFactory implements SqlParameterParserFactory {

    /** {@link nablarch.core.db.statement.SqlConvertor}のリスト */
    private List<SqlConvertor> sqlConvertors;

    /**
     * {@link nablarch.core.db.statement.BasicSqlParameterParser}を生成し返却する。
     *
     * @return SqlParameterParser BasicSqlParameterParserインスタンス
     */
    public SqlParameterParser createSqlParameterParser() {
        BasicSqlParameterParser parser = new BasicSqlParameterParser();
        if (sqlConvertors != null && !sqlConvertors.isEmpty()) {
            parser.setSqlConvertors(sqlConvertors);
        }
        return parser;
    }

    /**
     * SqlConvertorのリストを設定する。
     *
     * @param sqlConvertors SqlConvertorのリスト
     */
    public void setSqlConvertors(List<SqlConvertor> sqlConvertors) {
        this.sqlConvertors = sqlConvertors;
    }
}
