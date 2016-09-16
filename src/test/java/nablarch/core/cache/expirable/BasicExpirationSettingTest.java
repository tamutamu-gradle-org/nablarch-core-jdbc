package nablarch.core.cache.expirable;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Date;

import nablarch.core.cache.expirable.BasicExpirationSetting.TimeUnit;
import nablarch.core.date.SystemTimeProvider;
import nablarch.test.support.SystemRepositoryResource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;


/**
 * {@link BasicExpirationSettingTest}のテストクラス。
 *
 * @author T.Kawasaki
 */
public class BasicExpirationSettingTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/core/cache/expirable/BasicExpirationSettingTest.xml");

    /** テスト対象 */
    private BasicExpirationSetting target;

    private SystemTimeProvider systemTimeProvider;


    @Before
    public void setUp() throws Exception {
        target = repositoryResource.getComponent("expirationSetting");
        systemTimeProvider = repositoryResource.getComponent("systemTimeProvider");
    }

    /**
     * ミリ秒(ms)の評価ができること。
     */
    @Test
    public void testEvaluateMillSec() {

        Date evaluate = target.evaluate("10ms");
        Date now = systemTimeProvider.getDate();
        assertThat(now.before(evaluate), is(true));
        assertThat("差が10msであること。",
                evaluate.getTime() - now.getTime(), is(10L));
    }

    /** 時間単位として解釈できない文字列が渡された場合は例外が発生すること。 */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidExpression() {
        target.evaluate("invalid expression.");
    }

    /** 時間単位として存在しない単位が渡された場合は例外が発生すること。 */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeUnit() {
        target.evaluate("100year");  // yearという単位はない
    }

    /** 指定したキー（SQLID）に対応する有効期限が取得できること。 */
    @Test
    public void testExpiredDate() {
        Date expiredDate = target.getExpiredDate("please.change.me.tutorial.ss11AA.W11AA01Action#SELECT");
        Date now = systemTimeProvider.getDate();
        assertThat("差が100msであること。",
                expiredDate.getTime() - now.getTime(), is(100L));
    }

    /** プロパティsystemTimeProviderが設定されていない場合、例外が発生すること */
    @Test(expected = IllegalStateException.class)
    public void testSystemTimeProviderNotSet() {
        target.setSystemTimeProvider(null);   // 未設定の状態にする
        target.evaluate("10ms");
    }

    /** 登録されていないキー（SQLID）が指定された場合、例外が発生すること。 */
    @Test(expected = IllegalArgumentException.class)
    public void testGetExpiredDateNotRegistered() {
        target.getExpiredDate("NOT REGISTERED");
    }

    /** {@link java.lang.Enum}の暗黙メソッドのカバレッジを上げるテスト */
    @Test
    public void testTimeUnit() {
        assertThat(TimeUnit.valueOf("MILLISECOND"), is(TimeUnit.MILLISECOND));
        assertThat(TimeUnit.values().length, is(5));
    }

}
