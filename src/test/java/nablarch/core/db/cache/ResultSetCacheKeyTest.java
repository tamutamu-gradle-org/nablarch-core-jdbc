package nablarch.core.db.cache;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.containsString;

import nablarch.core.db.cache.statement.BoundParameters;
import nablarch.core.db.statement.ParameterHolder;

import org.junit.Before;
import org.junit.Test;

/**
 * {@link ResultSetCacheKey}のテストクラス。
 *
 * @author T.Kawasaki
 */
public class ResultSetCacheKeyTest {

    /** テスト対象 */
    private ResultSetCacheKey target;

    /** パラメータ */
    private ParameterHolder params = new ParameterHolder();

    @Before
    public void setUp() {
        params.add("name", "yamda");
        params.add("bytes", new byte[]{0x30, 0x31, 0x32, 0x34});
        target = new ResultSetCacheKey("SQL_ID", new BoundParameters(params), 5, 100);
    }

    /** 等値である場合、等価であること。 */
    @Test
    public void testEqualsSameInstance() {
        assertThat(target.equals(target), is(true));
    }

    /** 等価判定が成功すること。 */
    @Test
    public void testEqualsSuccess() {
        ResultSetCacheKey other = new ResultSetCacheKey("SQL_ID", new BoundParameters(params), 5, 100);
        assertThat(target.equals(other), is(true));
    }

    /** nullと比較した場合、等価とみなされないこと */
    @Test
    public void testEqualsFailNull() {
        assertThat("nullは常にfalse", target.equals(null), is(false));
    }

    /** 異なるクラスと比較した場合、等価とみなされないこと */
    @Test
    public void testEqualsFailDifferentClass() {
        assertThat("クラスが異なる", target.equals(new Object()), is(false));
    }

    /** SQL IDが異なる場合、等価とみなされないこと。*/
    @Test
    public void testSqlIdNotEquals() {
        ResultSetCacheKey other;
        other = new ResultSetCacheKey("NOT SAME ID!",  // not equal
                                      new BoundParameters(params),
                                      5,
                                      100);
        assertThat(target.equals(other), is(false));

    }

    /** パラメータが異なる場合、等価とみなされないこと。*/
    @Test
    public void testParamsNotEquals() {
        ParameterHolder notSameParams = new ParameterHolder();
        params.add("name", "yamda!");  // not equal
        params.add("bytes", new byte[]{0x30, 0x31, 0x32, 0x34});

        ResultSetCacheKey other = new ResultSetCacheKey("SQL_ID", new BoundParameters(notSameParams), 5, 100);
        assertThat(target.equals(other), is(false));
    }

    /** 開始位置が異なる場合、等価とみなされないこと。*/
    @Test
    public void testStartPosNotEquals() {
        ResultSetCacheKey other = new ResultSetCacheKey("SQL_ID",
                                                        new BoundParameters(params),
                                                        6,  // not equal
                                                        100);
        assertThat(target.equals(other), is(false));
    }

    /** 最大件数が異なる場合、等価とみなされないこと。*/
    @Test
    public void testMaxNotEquals() {
        ResultSetCacheKey other = new ResultSetCacheKey("SQL_ID",
                                                        new BoundParameters(params),
                                                        5,
                                                        101);  // not equal
        assertThat(target.equals(other), is(false));
    }

    /** 文字列表現のテスト */
    @Test
    public void testToString() {
        assertThat(target.toString(),
                   containsString("sqlId='SQL_ID', params={bytes=bytes, name=yamda}, startPos=5, max=100"));
    }
}
