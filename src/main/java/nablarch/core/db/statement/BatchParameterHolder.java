package nablarch.core.db.statement;

import nablarch.core.log.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * バッチ実行時のパラメータを組み立てるクラス。
 * {@link ParameterHolder}を複数集約し、
 * それらを整形する機能を持つ。
 *
 * @author T.Kawasaki
 */
public class BatchParameterHolder {

    /** バッチ実行されるパラメータ */
    private final List<ParameterHolder> list = new ArrayList<ParameterHolder>();

    /**
     * パラメータを追加する。
     *
     * @param params パラメータ
     */
    void add(ParameterHolder params) {
        list.add(params);
    }

    /**
     * {@inheritDoc}
     * 設定されたパラメータの文字列表現を返却する。
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            ParameterHolder holder = list.get(i);
            sb.append(Logger.LS);
            sb.append("\tbatch count = [").append(i + 1).append("]");
            holder.appendParametersForBatch(sb);
        }
        return sb.toString();
    }

    /** 設定されたパラメータをクリアする。 */
    void clear() {
        list.clear();
    }

    /**
     * 設定されたパラメータ数を取得する。
     *
     * @return パラメータ数。
     */
    int size() {
        return list.size();
    }

    /**
     * 何もしない{@link BatchParameterHolder}サブクラス。
     * パラメータを保持しておく必要がない場合は本クラスを使用する。
     * 本クラスを使用することで煩雑なnull判定を避けることができる。
     */
    static final class NopBatchParamHolder extends BatchParameterHolder {

        /** インスタンス（状態を持たないので共有可能） */
        private static final NopBatchParamHolder SOLO_INSTANCE = new NopBatchParamHolder();

        /**
         * インスタンスを取得する。
         *
         * @return インスタンス
         */
        static NopBatchParamHolder getInstance() {
            return SOLO_INSTANCE;
        }

        /** プライベートコンストラクタ。 */
        private NopBatchParamHolder() {
        }

        /**
         * {@inheritDoc}
         * 本クラスでは何もしない。
         */
        @Override
        void add(ParameterHolder params) {
        }
    }
}
