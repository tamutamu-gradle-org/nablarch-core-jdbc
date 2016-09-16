package nablarch.core.cache.example;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.transaction.SimpleDbTransactionExecutor;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.repository.SystemRepository;
import nablarch.core.repository.di.DiContainer;
import nablarch.core.repository.di.config.xml.XmlComponentDefinitionLoader;

public class ExampleMain {

    public static void main(String[] args) {
        initializeRepository("db-default.xml");

        new SimpleDbTransactionExecutor<Void>((SimpleDbTransactionManager) SystemRepository.get("tran")) {
            @Override
            public Void execute(AppDbConnection connection) {
                try {
                    connection.prepareStatement("drop table example_data").execute();
                } catch (Throwable t) {
                    // NOP
                }
                
                connection.prepareStatement("create table example_data(id varchar2(10) not null primary key, name varchar2(20) not null)").execute();
                
                connection.prepareStatement("insert into example_data(id, name) values('00001', 'name01')").execute();
                connection.prepareStatement("insert into example_data(id, name) values('00002', 'name02')").execute();
                connection.prepareStatement("insert into example_data(id, name) values('00003', 'name03')").execute();

                
                return null;
            }
        }.doTransaction();

        initializeRepository("nablarch/core/cache/example/example.xml");

        StaticDataUseExample example = SystemRepository.get("staticDataUseExample");
        example.getById("00001");
        example.getById("00002");
        example.getById("00003");

        example.getByName("name01");
        example.getByName("name02");
        example.getByName("name03");
    }

    private static void initializeRepository(String fileName) {
        SystemRepository.clear();
        SystemRepository.load(new DiContainer(new XmlComponentDefinitionLoader(fileName)));
    }
}
