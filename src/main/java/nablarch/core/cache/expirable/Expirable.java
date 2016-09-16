package nablarch.core.cache.expirable;

import java.util.Date;

import nablarch.core.util.annotation.Published;

/**
 * 有効期限付きのキャッシュ値を表すクラス。
 *
 * @param <V> 格納する値の型
 * @author T.Kawasaki
 */
@Published(tag = "architect")
public class Expirable<V> {

    /** キャッシュされる値 */
    private final V content;

    /** キャッシュ有効期限 */
    private final Date expiredDate;

    /**
     * コンストラクタ。
     *
     * @param content     キャッシュされる値（null不可）
     * @param expiredDate キャッシュ有効期限
     */
    Expirable(V content, Date expiredDate) {
        this.content = content;
        this.expiredDate = expiredDate;
    }

    /**
     * 格納されたキャッシュ値を取得する。
     *
     * @return キャッシュ値
     */
    V getContent() {
        return content;
    }

    /**
     * 有効期限日時を取得する。
     *
     * @return 有効期限日時
     */
    Date getExpiredDate() {
        return new Date(expiredDate.getTime());
    }

    /**
     * 有効期限切れかどうか判定する。
     * 現在日時＜有効期限が成り立つ場合、有効期限切れとする。
     *
     * @param now 判定基準となる現在日時
     * @return 有効期限切れの場合、真
     */
    boolean isExpired(Date now) {
        return now.after(expiredDate);
    }
}
