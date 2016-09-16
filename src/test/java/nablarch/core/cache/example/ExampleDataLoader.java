package nablarch.core.cache.example;

import java.util.ArrayList;
import java.util.List;

import nablarch.core.cache.StaticDataLoader;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.db.transaction.SimpleDbTransactionExecutor;
import nablarch.core.db.transaction.SimpleDbTransactionManager;

public class ExampleDataLoader implements StaticDataLoader<ExampleData> {
    /**
     * データロードに使用するSimpleDbTransactionManagerのインスタンス。
     */
    private SimpleDbTransactionManager dbManager;

    public ExampleData getValue(final Object id) {

        return new SimpleDbTransactionExecutor<ExampleData>(dbManager) {
            @Override
            public ExampleData execute(AppDbConnection connection) {
                // 永続化したオブジェクトをロード
                SqlPStatement stmt = connection
                        .prepareStatement("select id, name from example_data where id = ? order by id");
                stmt.setString(1, (String) id);
                SqlResultSet results = stmt.retrieve();
                if (results.size() > 0) {
                    SqlRow row = results.get(0);
                    ExampleData obj = createData(row);
                    return obj;
                } else {
                    return null;
                }
            }
        }.doTransaction();
    }

    public List<ExampleData> getValues(final String indexName, final Object key) {

        return new SimpleDbTransactionExecutor<List<ExampleData>>(dbManager) {
            @Override
            public List<ExampleData> execute(AppDbConnection connection) {
                if (indexName.equals("name")) {
                    String name = (String) key;
                    // 永続化したオブジェクトをロード
                    SqlPStatement stmt = connection.prepareStatement("select id, name from example_data where name = ? order by id");
                    stmt.setString(1, (String) name);
                    SqlResultSet results = stmt.retrieve();
                    List<ExampleData> objs = new ArrayList<ExampleData>();
                    for (SqlRow row : results) {
                        objs.add(createData(row));
                    }
                    return objs;
                } else {
                    throw new IllegalArgumentException("invalid indexName: indexName = " + indexName);
                }        
    
            }
        }.doTransaction();
    }

    public Object getId(ExampleData value) {
        // オブジェクトの持つidを取得する。
        return value.getId();
    }

    public Object generateIndexKey(String indexName, ExampleData value) {
        // オブジェクトからインデックスのキー値を取得する。
        if (indexName.equals("name")) {
            return value.getName();
        } else {
            throw new IllegalArgumentException(
                    "invalid indexName: indexName = " + indexName);
        }
    }

    public List<String> getIndexNames() {
        // インデックスの名称を返す。
        // この例ではインデックス"name"のみ作成している
        List<String> indexNames = new ArrayList<String>();
        indexNames.add("name");
        return indexNames;
    }

    public List<ExampleData> loadAll() {

        return new SimpleDbTransactionExecutor<List<ExampleData>>(dbManager) {
            @Override
            public List<ExampleData> execute(AppDbConnection connection) {
                // キャッシュにロードする全てのオブジェクトを取得する
                SqlPStatement stmt = connection
                        .prepareStatement("select id, name from example_data order by id");
                SqlResultSet results = stmt.retrieve();
                List<ExampleData> objs = new ArrayList<ExampleData>();
                for (SqlRow row : results) {
                    objs.add(createData(row));
                }
                return objs;
            }
        }.doTransaction();
    }

    private ExampleData createData(SqlRow row) {
        ExampleData obj = new ExampleData();
        obj.setId(row.getString("id"));
        obj.setName(row.getString("name"));
        return obj;
    }

    public void setDbManager(SimpleDbTransactionManager dbManager) {
        this.dbManager = dbManager;
    }
}
