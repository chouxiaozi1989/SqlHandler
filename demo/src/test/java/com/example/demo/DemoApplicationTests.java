package com.example.demo;

import org.caiyi.SqlHandler.core.Sql;
import org.caiyi.SqlHandler.util.BlobUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.sql.DataSource;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.HashMap;

@SpringBootTest
class DemoApplicationTests {
    @Autowired
    DataSource dataSource;

    @Test
    void contextLoads() {
        Sql sql = new Sql(dataSource);
        try {
            sql.setSql("select * from dojs.submissions where submission_id = ?");
            sql.setInt(1, 32966);
            ArrayList<HashMap<String, Object>> res = sql.executeQuery();
            System.out.println(new String(BlobUtil.getBytes((Blob)res.get(0).get("SUBMISSION_CODE"))));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
