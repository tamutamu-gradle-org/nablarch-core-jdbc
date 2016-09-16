package nablarch.core.cache.expirable;

import java.util.Date;

/**
 * 有効期限設定。
 * IDと有効期限の紐付けを行う。
 *
 * @author T.Kawasaki
 */
public interface ExpirationSetting {

    /**
     * 指定されたIDがキャッシュ対象か否かを判定する。
     *
     * @param id 判定対象となるID
     * @return キャッシュ対象である場合、真
     */
    boolean isCacheEnable(String id);

    /**
     * 指定されたIDの有効期限を取得する。
     *
     * @param id 判定対象となるID
     * @return 有効期限
     * @throws IllegalArgumentException 指定されたIDがキャッシュ対象でない場合
     */
    Date getExpiredDate(String id) throws IllegalArgumentException;
}
