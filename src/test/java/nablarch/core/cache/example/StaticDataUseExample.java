package nablarch.core.cache.example;

import java.util.List;

import nablarch.core.cache.StaticDataCache;


public class StaticDataUseExample {
    // DIによりStaticDataを設定
    private StaticDataCache<ExampleData> cache;

    public void setCache(StaticDataCache<ExampleData> cache) {
        this.cache = cache;
    }

    public void getById(String id) {
        // キャッシュからデータを取得する
        // 利用者は、キャッシュにデータがあるかを意識する必要はない
        ExampleData obj = cache.getValue(id);
        System.out.println("id = " + obj.getId());
        System.out.println("name = " + obj.getName());
    }

    public void getByName(String name) {
        // キャッシュからデータを取得する
        // 利用者は、キャッシュにデータがあるかを意識する必要はない
        List<ExampleData> objs = cache.getValues("name", name);
        for (ExampleData obj : objs) {
            System.out.println("id = " + obj.getId());
            System.out.println("name = " + obj.getName());
        }
    }
}
