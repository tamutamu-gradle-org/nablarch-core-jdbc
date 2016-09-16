package nablarch.core.db.statement;


import nablarch.core.db.connection.ConnectionFactory;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;

import org.junit.ClassRule;
import org.junit.runner.RunWith;


/**
 * SqlRowの自動テストクラス。
 *
 * @author Hisaaki Sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class SqlRowTest extends SqlRowTestLogic {

    @ClassRule
    public static SystemRepositoryResource systemRepositoryResource = new SystemRepositoryResource("db-default.xml");

    @Override
    protected ConnectionFactory createConnectionFactory() {
        return systemRepositoryResource.getComponentByType(ConnectionFactory.class);
    }
}

