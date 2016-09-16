package nablarch.core.db.cache.statement;


import nablarch.core.db.connection.BasicDbConnectionFactoryForDataSource;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.statement.BasicSqlPStatementTestLogic;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;

import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * {@link CacheableSqlPStatement}が{@link SqlPStatement}と
 * 互換性があることを確認するテストクラス。
 *
 * @author T.Kawasaki
 * @see BasicSqlPStatementTestLogic
 */
@RunWith(DatabaseTestRunner.class)
public class CacheableSqlPStatementCompatibilityTest extends BasicSqlPStatementTestLogic {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/core/db/statement/CacheableSqlPStatementCompatibilityTestConfiguration.xml");

    /** {@inheritDoc} */
    protected ConnectionFactory createConnectionFactory() {
        return repositoryResource.getComponentByType(BasicDbConnectionFactoryForDataSource.class);
    }
}
