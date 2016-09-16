package nablarch.core.db.cache;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.SQLException;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link InMemoryResultSetCache}のマルチスレッドテスト。
 *
 * @author T.Kawasaki
 */
@RunWith(DatabaseTestRunner.class)
public class InMemoryResultSetCacheMultiTest {

    @ClassRule
    public static SystemRepositoryResource systemRepositoryResource = new SystemRepositoryResource(
            "nablarch/core/db/cache/statement/CacheableStatementFactoryTest.xml");

    /** SQL IDのプレフィックス */
    private static final String PREFIX
            = "nablarch/core/db/cache/statement/InMemoryResultSetCacheMultiTest#";

    /** 呼び出し回数 */
    private static final int CALL_COUNT = 100;

    /** キャッシュ */
    private ResultSetCache cache;

    @Rule
    public MultiThreadResource threads = new MultiThreadResource(16);  // 16 threads

    @BeforeClass
    public static void beforeClass() {
        VariousDbTestHelper.createTable(InMemoryResultsetCacheTestEntity.class);
        VariousDbTestHelper.setUpTable(
                new InMemoryResultsetCacheTestEntity(1L)
        );
    }

    @Before
    public void setUp() {
        cache = systemRepositoryResource.getComponent("resultSetCache");
        cache.clear();
    }

    /**
     * getとclearを混在させて実行。
     *
     * @throws SQLException 発生しない
     */
    @Test(timeout = 30 * 1000)
    public void test() throws SQLException {
        for (int i = 0; i < CALL_COUNT; i++) {
            if (i % 7 == 0) {
                executeClear();
            }
            executeGet();
            }
        threads.terminateAndWait(20);
    }

    private void executeClear() {
        threads.service.execute(new Runnable() {
            @Override
            public void run() {
                cache.clear();
            }
        });
    }

    private void executeGet() {
        final ConnectionFactory factory = systemRepositoryResource.getComponent("connectionFactory");
        final String sqlId = PREFIX + "TEST_NO_PARAM";
        threads.service.execute(new Runnable() {
            @Override
            public void run() {
                final TransactionManagerConnection connection = factory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
                try {
                    SqlPStatement stmt = connection.prepareStatementBySqlId(sqlId);
                    final SqlResultSet rs = stmt.retrieve();
                    assertThat(rs.size(), is(1));
                    assertThat(rs.get(0)
                            .getString("MSG"), is("HELLO"));
                } finally {
                    connection.terminate();
                }
            }
        });
    }

    @Entity
    @Table(name = "IN_MEMORY_RESULTSET_CACHE_TEST")
    public static class InMemoryResultsetCacheTestEntity {

        @Column(name = "key_col", length = 5)
        @Id
        public Long id;

        public InMemoryResultsetCacheTestEntity() {
        }

        public InMemoryResultsetCacheTestEntity(Long id) {
            this.id = id;
        }
    }
}

