package nablarch.core.db.cache.statement;

import java.util.Collections;
import java.util.Map;

import nablarch.core.cache.expirable.BasicExpirationSetting;
import nablarch.core.date.SystemTimeProvider;
import nablarch.core.db.cache.InMemoryResultSetCache;
import nablarch.core.db.statement.BasicStatementFactory;
import nablarch.core.db.statement.BasicStatementFactoryTestLogic;
import nablarch.util.FixedSystemTimeProvider;

/**
 * {@link CacheableStatementFactory}が{@link BasicStatementFactory}と
 * 互換性があることを確認するテストクラス。
 *
 * @author T.Kawasaki
 * @see BasicStatementFactoryTestLogic
 */
public class CacheableStatementFactoryCompatibilityTest extends BasicStatementFactoryTestLogic {

    @Override
    protected BasicStatementFactory createStatementFactory() {

        CacheableStatementFactory factory = new CacheableStatementFactory();

        SystemTimeProvider timeProvider = new FixedSystemTimeProvider("20101231012345");

        // キャッシュ
        InMemoryResultSetCache cache = new InMemoryResultSetCache();
        cache.setCacheSize(10);
        cache.setSystemTimeProvider(timeProvider);
        factory.setResultSetCache(cache);

        // 有効期限設定
        BasicExpirationSetting exp = new BasicExpirationSetting();
        exp.setSystemTimeProvider(timeProvider);
        exp.setExpirationList(Collections.<Map<String, String>>emptyList());
        factory.setExpirationSetting(exp);

        return factory;
    }
}
