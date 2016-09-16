package nablarch.core.db;

import nablarch.core.db.DbAccessIntegrationTest.DbAccessJndiTest;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;



/**
 * データベースアクセス機能全般のテストクラス。
 *
 * @author Hisaaki Sioiri
 */
@RunWith(Suite.class)
@SuiteClasses({DbAccessIntegrationTest.DbAccessTest.class, DbAccessJndiTest.class})
public class DbAccessIntegrationTest {

    @RunWith(DatabaseTestRunner.class)
    public static class DbAccessTest extends DbAccessTestLogic {

        @Override
        protected String getConfigUrl() {
            return "nablarch/core/db/db-test.xml";
        }
    }


    @RunWith(DatabaseTestRunner.class)
    @TargetDb(include = TargetDb.Db.ORACLE)
    public static class DbAccessJndiTest extends DbAccessTestLogic.ConfigurationsTest {

        @Override
        protected String getConfigUrl() {
            return "nablarch/core/db/db-test2.xml";
        }
    }

}

