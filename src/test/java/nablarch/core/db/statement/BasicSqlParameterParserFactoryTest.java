package nablarch.core.db.statement;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import nablarch.core.db.statement.sqlconvertor.VariableInSyntaxConvertor;

import org.junit.Test;

import mockit.Deencapsulation;

/**
 * {@link BasicSqlParameterParserFactory}のテストクラス。
 */
public class BasicSqlParameterParserFactoryTest {

    private BasicSqlParameterParserFactory sut = new BasicSqlParameterParserFactory();

    /**
     * デフォルト設定の場合
     */
    @Test
    public void createSqlParameterParser() throws Exception {
        SqlParameterParser parser = sut.createSqlParameterParser();
        assertThat(parser, is(instanceOf(BasicSqlParameterParser.class)));
    }

    /**
     * {@link SqlConvertor}を設定した場合
     */
    @Test
    public void createSqlParameterParserFromCustomSetting() throws Exception {
        sut.setSqlConvertors(Collections.<SqlConvertor>singletonList(new VariableInSyntaxConvertor()));
        final SqlParameterParser parser = sut.createSqlParameterParser();
        final SqlConvertor[] convertors = Deencapsulation.getField(parser, "sqlConvertors");
        assertThat(convertors.length, is(1));
        assertThat(convertors[0], is(instanceOf(VariableInSyntaxConvertor.class)));
    }
}