package nablarch.core.db.statement;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import nablarch.core.db.connection.BasicDbConnectionFactoryForDataSource;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.statement.BatchParameterHolder.NopBatchParamHolder;
import nablarch.core.db.statement.ParameterHolder.NopParameterHolder;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link nablarch.core.db.statement.BasicSqlPStatement}のテストクラス。
 *
 * @author Hisaaki Sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class BasicSqlPStatementTest extends BasicSqlPStatementTestLogic {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/core/db/statement/BasicSqlPStatementTestConfiguration.xml");

    @Override
    protected ConnectionFactory createConnectionFactory() {
        return repositoryResource.getComponentByType(BasicDbConnectionFactoryForDataSource.class);
    }

    @Test
    public void testOnTraceLogDisabled() {
        BasicSqlPStatement target = new BasicSqlPStatement("sql", null) {
            @Override
            protected boolean isTraceLogEnabled() {
                return false;
            }
        };

        assertThat(target.createParamHolder(),
                   instanceOf(NopParameterHolder.class));

        assertThat(target.createBatchParamHolder(),
                   instanceOf(NopBatchParamHolder.class));
    }
}
