package com.neoremind.mrsample.hbase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by helechen on 16/12/5.
 */
public class HBaseDAOTest {

    private static HBaseDAO hBaseDAO;

    @BeforeClass
    public static void setUp() throws IOException {
        hBaseDAO = new HBaseDAO();
        hBaseDAO.createTable("mytest", new String[]{"cf1", "cf2"});
    }

    @AfterClass
    public static void clear() throws IOException {
        hBaseDAO.deleteTable("mytest");
    }

    @Test
    public void testListTables() throws IOException {
        hBaseDAO.listTables();
    }

    @Test
    public void testInsertAndGet() throws IOException {
        hBaseDAO.insert("mytest", "user1", "cf1", "show", "100");
        hBaseDAO.insert("mytest", "user1", "cf1", "clk", "200");
        hBaseDAO.insert("mytest", "user1", "cf1", "cost", "300");
        hBaseDAO.insert("mytest", "user2", "cf1", "show", "600");
        hBaseDAO.insert("mytest", "user2", "cf2", "clk", "400");
        hBaseDAO.insert("mytest", "user2", "cf2", "cost", "500");
        hBaseDAO.insert("mytest", "user3", "cf1", "show", "700");

        hBaseDAO.getData("mytest", "user1");
    }

}
