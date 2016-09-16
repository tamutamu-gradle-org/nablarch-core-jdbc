package nablarch.core.cache.expirable;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Date;

import nablarch.core.util.DateUtil;

import org.junit.Test;

/**
 * {@link Expirable}のテストクラス。
 *
 * @author T.Kawasaki
 */
public class ExpirableTest {

    /** テスト対象 */
    private Expirable<String> target = new Expirable<String>("content", DateUtil.getDate("20140101"));

    /** 内包する値が取得できること。 */
    @Test
    public void testGetContent() {
        assertThat(target.getContent(), is("content"));
    }

    /**
     * 渡された現在日時が有効期限より過去である場合、
     * 有効期限切れ判定が偽を返却すること。
     */
    @Test
    public void testNotExpire() {

        Date now;
        now = DateUtil.getDate("20131231");
        assertThat(target.isExpired(now), is(false));

        now = DateUtil.getDate("20140101");
        assertThat(target.isExpired(now), is(false));
    }

    /**
     * 渡された現在日時が有効期限より未来である場合、
     * 有効期限切れ判定が偽を返却すること。
     */
    @Test
    public void testExpire() {
        // 期限切れ
        Date now = DateUtil.getDate("20140102");
        assertThat(target.isExpired(now), is(true));
    }

    /** 有効期限日時を取得できること。*/
    @Test
    public void testGetDate() {
        assertThat(target.getExpiredDate(), is(DateUtil.getDate("20140101")));
    }
}
