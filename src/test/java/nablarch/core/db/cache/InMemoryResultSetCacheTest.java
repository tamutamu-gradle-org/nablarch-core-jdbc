package nablarch.core.db.cache;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.Date;
import java.util.List;

import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.util.DateUtil;
import nablarch.test.support.log.app.OnMemoryLogWriter;
import nablarch.util.FixedSystemTimeProvider;

import org.junit.Before;
import org.junit.Test;

/**
 * {@link InMemoryResultSetCache}のテストクラス。
 *
 * @author T.Kawasaki
 */
public class InMemoryResultSetCacheTest {

    /** ログライター名 */
    private static final String WRITER_NAME = "writer.memory";

    /** テスト対象 */
    private InMemoryResultSetCache target = new InMemoryResultSetCache();

    /** システム日時 */
    private FixedSystemTimeProvider timeProvider = new FixedSystemTimeProvider("20140101000000");

    /** キャッシュキー */
    private ResultSetCacheKey key = new ResultSetCacheKeyBuilder("001").addParam("name", "yamada").build();



    @Before
    public void setUp() {
        target.setCacheSize(10);
        target.setSystemTimeProvider(timeProvider);
        OnMemoryLogWriter.clear();
    }

    /** キャッシュにエントリを追加できること。 */
    @Test
    public void testAdd() {

        SqlResultSet notCached = target.getIfNotExpired(key);
        assertThat(notCached, is(nullValue()));

        SqlResultSet registered = new MockSqlResultSet();
        Date expire = DateUtil.getDate("20140101");
        target.add(key, registered, expire);

        SqlResultSet notExpired = target.getIfNotExpired(key);
        assertThat("キャッシュに追加されている",
                   notExpired, is(not(nullValue())));

    }

    /** 取得対象のエントリが有効期限切れの場合、nullが返却されること */
    @Test
    public void testExpired() {
        SqlResultSet registered = new MockSqlResultSet();
        Date expire = DateUtil.getDate("20131231");  // 有効期限切れ
        target.add(key, registered, expire);

        SqlResultSet expired = target.getIfNotExpired(key);
        assertThat(expired, is(nullValue()));
    }


    /** ロガーが有効な場合のテスト */
    @Test
    public void testWhenLoggerEnable() {
        target.setCacheSize(1);

        // １つめを登録
        ResultSetCacheKey one = new ResultSetCacheKeyBuilder("SQL_001").build();
        target.add(one, new MockSqlResultSet(), DateUtil.getDate("20140101"));
        assertLog("cache entry added:",
                  "key=[sqlId='SQL_001', params={}, startPos=1, max=0}]",
                  "expire=[2014/01/01 12:00:00:000]"
        );
        clearLog();


        // ２つめを登録（１つめは押し出される）
        ResultSetCacheKey two = new ResultSetCacheKeyBuilder("SQL_002").build();
        target.add(two, new MockSqlResultSet(), DateUtil.getDate("20140102"));
        assertLog("cache entry added:",
                  "key=[sqlId='SQL_002', params={}, startPos=1, max=0}]",
                  "expire=[2014/01/02 12:00:00:000]"
        );
        assertLog("the eldest entry removed:");
        clearLog();


        // １つめをremove（存在しない）
        target.remove(one);
        assertThat(getLog().isEmpty(), is(true));  // ログ出力されていない

        // ２つめをremove（存在する）
        target.remove(two);
        assertLog("cache entry removed:",
                  "key=[sqlId='SQL_002', params={}, startPos=1, max=0}]"
        );

        // 期限切れ
        target.add(one, new MockSqlResultSet(),
                   DateUtil.getDate("20131231"));  // 期限切れ
        clearLog();
        SqlResultSet expired = target.getIfNotExpired(one);
        assertThat(expired, is(nullValue()));
        assertLog("cache entry expired",
                  "key=[sqlId='SQL_001', params={}, startPos=1, max=0}]",
                  "expire=[2013/12/31 12:00:00:000]",
                  "current=[2014/01/01 12:00:00:000]"
        );
        clearLog();

        // クリア
        target.clear();
        assertLog("cache cleared");
    }

    /** ロガーが無効な場合のテスト */
    @Test
    public void testLoggerOff() {
        target = new LoggerDisabledInMemoryResultSetCache();
        target.setCacheSize(100);
        target.setSystemTimeProvider(timeProvider);

        ResultSetCacheKey one = new ResultSetCacheKeyBuilder("SQL_001").build();
        target.add(one, new MockSqlResultSet(), DateUtil.getDate("20140101"));
        assertThat(getLog().isEmpty(), is(true));  // ログ出力されていない
    }

    /**
     * 期待する文言が全てログ出力されていることを表明する。
     *
     * @param expected 期待する文言（複数指定可）
     */
    private void assertLog(String... expected) {
        OnMemoryLogWriter.assertLogContains(WRITER_NAME, expected);
    }

    /**
     * ログ出力された文言を取得する。
     *
     * @return ログ出力された文言
     */
    private List<String> getLog() {
        return OnMemoryLogWriter.getMessages(WRITER_NAME);
    }

    /** ログを明示的にクリアする。 */
    private void clearLog() {
        OnMemoryLogWriter.clear();
    }

    /**
     * ロガーを強制的に無効にした{@link InMemoryResultSetCache}サブクラス。
     */
    private static class LoggerDisabledInMemoryResultSetCache extends InMemoryResultSetCache {
        @Override
        boolean isLoggerEnabled() {
            return false;
        }
    }

    /** {@link SqlResultSet}のモッククラス。 */
    static class MockSqlResultSet extends SqlResultSet {
        MockSqlResultSet() {
            super(0);
        }
    }
}
