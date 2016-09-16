package nablarch.core.db.dialect;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * Dialectテスト用のテーブル
 */
@Entity
@Table(name = "DIALECT")
public class DialectEntity {

    @Id
    @Column(name = "entity_id", length = 18, nullable = false)
    public Long id;

    @Column(name = "str", length = 10)
    public String string;

    @Column(name = "num", length = 9)
    public Integer numeric;

    @Column(name = "big_int", length = 10)
    public Long bigInt;

    @Column(name = "date_col", columnDefinition = "date")
    @Temporal(TemporalType.DATE)
    public Date date;

    @Column(name = "timestamp_col", columnDefinition = "timestamp")
    public Timestamp timestamp;

    @Column(name = "decimal_col", precision = 15, scale = 5)
    public BigDecimal decimal;

    @Column(name = "binary_col")
    public byte[] binary;

    public DialectEntity() {
    }

    public DialectEntity(Long id, String str) {
        this.id = id;
        this.string = str;
    }

    public DialectEntity(
            Long id, String string, Integer numeric, Long bigInt, Date date, BigDecimal decimal,
            Timestamp timestamp, byte[] binary) {
        this.id = id;
        this.string = string;
        this.numeric = numeric;
        this.bigInt = bigInt;
        this.date = date;
        this.decimal = decimal;
        this.timestamp = timestamp;
        this.binary = binary;
    }
}
