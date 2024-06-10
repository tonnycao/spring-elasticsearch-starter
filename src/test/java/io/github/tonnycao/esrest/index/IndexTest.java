package io.github.tonnycao.esrest.index;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class IndexTest {

    @Autowired
    IndexOps indexOps;

    @Test
    void create() throws IOException {
      Boolean result =  indexOps.create("test_index_setting");
      assertThat(result).isEqualTo(true);
    }

    @Test
    void delete() throws IOException {
        Boolean result =  indexOps.delete("test_index");
        assertThat(result).isEqualTo(true);
    }

    @Test
    void checkExist() throws IOException {
        Boolean result =  indexOps.checkExist("test_index");
        assertThat(result).isEqualTo(false);
    }

    @Test
    void queryOne() throws IOException {
        String result =  indexOps.queryOne("test_index_setting");
        System.out.println(result);
    }

    @Test
    void putIndexMapping() throws IOException {
        String name = "test_index";
        Map<String, Map<String, Object>> properties = new HashMap<>();
        Map<String, Object> s = new HashMap<>();
        s.put("index.number_of_shards", 1);
        s.put("index.number_of_replicas", 0);
        Map<String, Object> message = new HashMap<>();
        message.put("type", "text");
        properties.put("message", message);

        Boolean result = indexOps.create(name, properties, s);

        assertThat(result).isEqualTo(true);
    }
}