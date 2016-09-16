package nablarch.core.cache.expirable;

import static nablarch.core.util.DateUtil.getDate;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.Date;

import nablarch.util.FixedSystemTimeProvider;

import org.junit.Before;
import org.junit.Test;

/**
 * {@link InMemoryExpirableCache}のテストクラス。
 *
 * @author T.Kawasaki
 */
public class InMemoryExpirableCacheTest {

    /** テスト対象 */
    private InMemoryExpirableCache<Integer, String> target = new InMemoryExpirableCache<Integer, String>();

    /** テスト用のリスナー */
    private MockListener listener = new MockListener();

    @Before
    public void setUp() {
        target.setSystemTimeProvider(new FixedSystemTimeProvider("20140101000000"));
        target.setCacheSize(3);
        target.setCacheListener(listener);
    }

    /** 有効期限切れでないエントリから値が取得できること。 */
    @Test
    public void testGetSuccess() {
        Integer k = 1;
        String v = "1";
        target.add(k, v, getDate("20140101"));
        assertThat(target.getIfNotExpired(1), is("1"));
        assertThat("コールバックされていること。",
                   listener.added, is(true));
    }

    /**
     * キャッシュにないエントリのキーが指定された場合、
     * nullが返却されること。
     */
    @Test
    public void testGetNotAddedValue() {
        assertThat(target.getIfNotExpired(Integer.MIN_VALUE), is(nullValue()));
        assertThat("コールバックされていること。",
                   listener.miss, is(true));
    }

    /**
     * 指定されたエントリが有効期限切れの場合、nullが返却されること。
     */
    @Test
    public void testGetFailExpired() {
        Integer k = 1;
        String v = "1";
        target.add(k, v, getDate("20131231"));
        assertThat("現在日時は2014/1/1なので有効期限切れ",
                   target.getIfNotExpired(1), is(nullValue()));
        assertThat("コールバックされていること。",
                   listener.expired, is(true));
    }

    /** 有効期限指定なしでキャッシュに追加されたエントリが取得できること。 */
    @Test
    public void testGetUnlimited() {
        Integer k = 1;
        String v = "1";
        target.addUnlimited(k, v);
        assertThat(target.getIfNotExpired(1), is("1"));
        assertThat("コールバックされていること。",
                   listener.added, is(true));
    }

    /** プロパティcacheSizeが設定されていない場合、例外が発生すること。 */
    @Test(expected = IllegalStateException.class)
    public void testSystemTimeProviderNotSet() {
        InMemoryExpirableCache<Integer, String> target = new InMemoryExpirableCache<Integer, String>();
        target.setSystemTimeProvider(new FixedSystemTimeProvider("20140101000000"));

        Integer k = 1;
        String v = "1";
        target.add(k, v, getDate("20140102"));
    }

    /** プロパティsystemTimeProviderが設定されていない場合、例外が発生すること。 */
    @Test(expected = IllegalStateException.class)
    public void testCacheNotSet() {
        InMemoryExpirableCache<Integer, String> target = new InMemoryExpirableCache<Integer, String>();
        target.setCacheSize(3);

        Integer k = 1;
        String v = "1";
        target.add(k, v, getDate("20140102"));

    }

    /** 指定したキーに合致するエントリをキャッシュから削除できること。 */
    @Test
    public void testRemove() {
        Integer k = 1;
        String v = "1";
        target.add(k, v, getDate("20140101"));
        assertThat(target.getIfNotExpired(1), is("1"));

        target.remove(1);
        assertThat(target.getIfNotExpired(1), is(nullValue()));
        assertThat("コールバックされていること。",
                   listener.removed, is(true));
    }

    /**
     * コールバックメソッドのテスト。
     * 削除時に削除対象エントリがnullの場合、
     * （削除時に、削除対象となるエントリが存在しなかった場合）
     * コールバックがされないこと。
     * （すでにコールバック済みであるはずなので）
     */
    @Test
    public void testCallListenerOnRemove() {
        Integer k = 1;
        target.callListenerOnRemove(k, null);
        assertThat("コールバックされていないこと。",
                   listener.removed, is(false));
    }

    /** キャッシュ全クリアした場合、全エントリが削除されること。 */
    @Test
    public void testClear() {
        {
            Integer k = 1;
            String v = "1";
            target.add(k, v, getDate("20140101"));
            assertThat(target.getIfNotExpired(1), is("1"));
        }

        {
            Integer k = 2;
            String v = "2";
            target.add(k, v, getDate("20140101"));
            assertThat(target.getIfNotExpired(2), is("2"));
        }

        target.clear();

        assertThat(target.getIfNotExpired(1), is(nullValue()));
        assertThat(target.getIfNotExpired(2), is(nullValue()));
        assertThat("コールバックされていること。",
                   listener.cleared, is(true));

    }

    /**
     * テスト用のモックリスナー。
     * コールバックされた時、各イベントに対応するフラグが設定される。
     */
    private static class MockListener implements ExpirableCacheListener<Integer> {
        boolean hit;
        boolean miss;
        boolean expired;
        boolean added;
        boolean removed;
        boolean cleared;

        @Override
        public void onCacheHit(Integer key, Date now) {
            hit = true;
        }

        @Override
        public void onCacheNotHit(Integer key) {
            miss = true;
        }

        @Override
        public void onExpire(Integer key, Date now, Date expiredDate) {
            expired = true;
        }

        @Override
        public void onCacheAdded(Integer key, Date expiredDate) {
            added = true;
        }

        @Override
        public void onRemove(Integer key) {
            removed = true;
        }

        @Override
        public void onClear() {
            cleared = true;
        }
    }

}
