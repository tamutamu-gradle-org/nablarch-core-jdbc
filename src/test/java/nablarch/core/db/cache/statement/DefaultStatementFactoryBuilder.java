package nablarch.core.db.cache.statement;

import java.util.ArrayList;
import java.util.Map;

import nablarch.core.cache.expirable.BasicExpirationSetting;
import nablarch.core.date.SystemTimeProvider;
import nablarch.core.db.cache.InMemoryResultSetCache;
import nablarch.core.db.cache.ResultSetCache;
import nablarch.core.db.statement.BasicSqlLoader;
import nablarch.core.db.statement.exception.BasicSqlStatementExceptionFactory;
import nablarch.util.FixedSystemTimeProvider;

/**
 * 各テストで使用する{@link CacheableStatementFactory}を生成するクラス。
 *
 * @author T.Kawasaki
 */
class DefaultStatementFactoryBuilder {

    private SystemTimeProvider systemTimeProvider = new FixedSystemTimeProvider("20100101000000");

    CacheableStatementFactory createStatementFactory() {
        CacheableStatementFactory factory = new CacheableStatementFactory();
        //factory.setResultSetConvertor(new SimpleResultSetConvertor());
        factory.setSqlStatementExceptionFactory(new BasicSqlStatementExceptionFactory());        factory.setExpirationSetting(createSetting());
        factory.setResultSetCache(createCache());
        factory.setSqlLoader(new BasicSqlLoader());
        return factory;
    }

    ResultSetCache createCache() {
        InMemoryResultSetCache cache = new InMemoryResultSetCache();
        cache.setSystemTimeProvider(systemTimeProvider);
        cache.setCacheSize(10);
        return cache;
    }


    BasicExpirationSetting createSetting() {
        BasicExpirationSetting setting = new BasicExpirationSetting();
        setting.setExpirationList(new ArrayList<Map<String, String>>());
        setting.setSystemTimeProvider(systemTimeProvider);
        return setting;
    }
}
