package nablarch.core.db.statement.autoproperty;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;

/**
 * {@link FieldAndAnnotationLoader}のテストクラス。
 *
 * @author Hisaaki Sioiri
 */
public class FieldAndAnnotationLoaderTest {

    /**
     * {@link FieldAndAnnotationLoader#getValue(Object)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testGetValue() throws Exception {

        /** Fieldオブジェクトの取得確認 */
        FieldAndAnnotationLoader loader = new FieldAndAnnotationLoader();
        Map<String, Map<String, Object>> fieldMap = loader.getValue(TestEntity.class);

        Field userId = (Field) fieldMap.get("userId").get("FIELD");
        assertEquals("userId", userId.getName());
        assertEquals(String.class, userId.getType());
        assertEquals("Annotationはないのでサイズ0", 0, ((Annotation[]) fieldMap.get("userId").get("ANNOTATION")).length);

        Field num1 = (Field) fieldMap.get("num1").get("FIELD");
        assertEquals("num1", num1.getName());
        assertEquals("サブクラスに同名の変数があるため、サブクラスの型になる。", Long.class, num1.getType());
        assertEquals("Annotationはないのでサイズ0", 0, ((Annotation[]) fieldMap.get("num1").get("ANNOTATION")).length);

        Field num2 = (Field) fieldMap.get("num2").get("FIELD");
        assertEquals("num2", num2.getName());
        assertEquals("Annotationはないのでサイズ0", 0, ((Annotation[]) fieldMap.get("num2").get("ANNOTATION")).length);
        assertEquals(int.class, num2.getType());

        // Filedとアノテーションの確認
        fieldMap = loader.getValue(SubEntity.class);

        // userId
        Map<String, Object> userIdInfo = fieldMap.get("userId");
        assertEquals("userId", ((Field) userIdInfo.get("FIELD")).getName());
        assertEquals(String.class, ((Field) userIdInfo.get("FIELD")).getType());
        assertEquals("サブクラスにはアノテーションがないので、サイズは0", 0, ((Annotation[]) userIdInfo.get("ANNOTATION")).length);

        // date
        Map<String, Object> dateInfo = fieldMap.get("date");
        assertEquals("date", ((Field) dateInfo.get("FIELD")).getName());
        assertEquals(String.class, ((Field) dateInfo.get("FIELD")).getType());

        List<Annotation> list = Arrays.asList((Annotation[]) dateInfo.get("ANNOTATION"));
        assertEquals(1, list.size());
        assertEquals(CurrentDateTime.class, list.get(0).annotationType());

        // dateTime
        Map<String, Object> dateTimeInfo = fieldMap.get("dateTime");
        assertEquals("dateTime", ((Field) dateTimeInfo.get("FIELD")).getName());
        assertEquals(String.class, ((Field) dateTimeInfo.get("FIELD")).getType());

        list = Arrays.asList((Annotation[]) dateTimeInfo.get("ANNOTATION"));
        assertEquals(1, list.size());
        assertEquals(CurrentDateTime.class, list.get(0).annotationType());
        assertEquals("yyyyMMddHHmmss", ((CurrentDateTime) list.get(0)).format());
    }

    /**
     * {@link FieldAndAnnotationLoader#getValues(String, Object)} のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testGetValues() throws Exception {

        FieldAndAnnotationLoader loader = new FieldAndAnnotationLoader();
        try {
            loader.getValues(null, null);
            fail("ここはとおらない。");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("FieldAndAnnotationLoader#getValues is unsupported."));
        }
    }

    /**
     * {@link FieldAndAnnotationLoader#loadAll()} のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testLoadAll() throws Exception {
        FieldAndAnnotationLoader loader = new FieldAndAnnotationLoader();
        try {
            loader.loadAll();
            fail("ここはとおらない。");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("FieldAndAnnotationLoader#loadAll is unsupported."));
        }
    }

    /**
     * {@link FieldAndAnnotationLoader#getIndexNames()} のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testGetIndexNames() throws Exception {
        FieldAndAnnotationLoader loader = new FieldAndAnnotationLoader();
        try {
            loader.getIndexNames();
            fail("ここはとおらない。");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("FieldAndAnnotationLoader#getIndexNames is unsupported."));
        }
    }

    /**
     * {@link FieldAndAnnotationLoader#getId(java.util.Map)} のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testGenerateId() throws Exception {
        FieldAndAnnotationLoader loader = new FieldAndAnnotationLoader();
        try {
            loader.getId(null);
            fail("ここはとおらない。");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("FieldAndAnnotationLoader#generateId is unsupported."));
        }
    }

    /**
     * {@link FieldAndAnnotationLoader#generateIndexKey(String, java.util.Map)} のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testGenerateIndexKey() throws Exception {
        FieldAndAnnotationLoader loader = new FieldAndAnnotationLoader();
        try {
            loader.generateIndexKey(null, null);
            fail("ここはとおらない。");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("FieldAndAnnotationLoader#generateIndexKey is unsupported."));
        }
    }

    //*****************************************************
    // テスト用オブジェクト
    //*****************************************************

    private static class BaseTestEntity {
        private String userId;
        private Integer num1;
        private int num2;
    }

    private static class TestEntity extends BaseTestEntity {
        private Long num1;
    }

    private static class BaseEntity {
        @UserId
        private String userId;

        @CurrentDateTime
        private String date;

        @CurrentDateTime(format = "yyyyMMddHHmmss")
        private String dateTime;

        // アノテーションの存在しないフィールド
        private String data1;
        private int data2;
        private List data3;
    }

    private static class SubEntity extends BaseEntity {
        private String userId;
    }
}

