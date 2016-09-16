package nablarch.core.db.cache;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import nablarch.core.db.cache.statement.BoundParameters;
import nablarch.core.db.statement.ParameterHolder;

import org.junit.Test;

/**
 * {@link ResultSetCacheKeyBuilder}のテストクラス。
 *
 * @author T.Kawasaki
 */
public class ResultSetCacheKeyBuilderTest {

    /** 名前付きパラメータを生成できること */
    @Test
    public void testNamedEqualsSuccess() {
        ParameterHolder params = new ParameterHolder();
        params.add("name", "yamda");
        params.add("bytes", new byte[]{0x30, 0x31, 0x32, 0x34});
        ResultSetCacheKey expected = new ResultSetCacheKey("SQL_ID", new BoundParameters(params), 5, 100);

        // 上記と等価なオブジェクトを生成する。
        ResultSetCacheKey actual = new ResultSetCacheKeyBuilder("SQL_ID")
                .addParam("name", "yamda")
                .addParam("bytes", new byte[] { 0x30, 0x31, 0x32, 0x34})
                .setMax(100)
                .setStartPos(5)
                .build();

        assertThat(actual.equals(expected), is(true));
    }

    /** インデックス指定パラメータを作成できること */
    @Test
    public void testIndexedEqualsSuccess() {
        ParameterHolder params = new ParameterHolder();
        params.add(1, "yamda");
        params.add(2, new byte[]{0x30, 0x31, 0x32, 0x34});
        ResultSetCacheKey expected = new ResultSetCacheKey("SQL_ID", new BoundParameters(params), 5, 100);

        // 上記と等価なオブジェクトを生成する。
        ResultSetCacheKey actual = new ResultSetCacheKeyBuilder("SQL_ID")
                .addParam(1, "yamda")
                .addParam(2, new byte[]{0x30, 0x31, 0x32, 0x34})
                .setMax(100)
                .setStartPos(5)
                .build();
        assertThat(actual.equals(expected), is(true));
    }

}
