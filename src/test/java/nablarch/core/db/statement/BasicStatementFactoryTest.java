package nablarch.core.db.statement;

/**
 * {@link BasicStatementFactory}のテストクラス。
 *
 * @author hisaaki sioiri
 */
public class BasicStatementFactoryTest extends BasicStatementFactoryTestLogic {

    @Override
    protected BasicStatementFactory createStatementFactory() {
        return new BasicStatementFactory();
    }
}
