package com.neoremind.mrsample.hbase.client.example;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * hbase.coprocessor.region.classes for RegionObservers and Endpoints.
 * <p>
 * hbase.coprocessor.wal.classes for WALObservers.
 * <p>
 * hbase.coprocessor.master.classes for MasterObservers.
 * <p>
 * <value> must contain the fully-qualified class name of your coprocessor’s implementation class.
 * <p>
 * For example to load a Coprocessor (implemented in class SumEndPoint.java) you have to create following entry in
 * RegionServer’s 'hbase-site.xml' file (generally located under 'conf' directory):
 * <p>
 * <property>
 * <name>hbase.coprocessor.region.classes</name>
 * <value>org.myname.hbase.coprocessor.endpoint.SumEndPoint</value>
 * </property>
 * <p>
 * An example of using the {@link BufferedMutator} interface.
 */
public class BufferedMutatorExample2 extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(BufferedMutatorExample2.class);

    public static final int TASK_COUNT = 2;
    private static final TableName TABLE = TableName.valueOf("mytest");
    private static final byte[] FAMILY = Bytes.toBytes("cf2");

    @Override
    public int run(String[] args) throws InterruptedException, ExecutionException, TimeoutException {

        /** a callback invoked when an asynchronous write fails. */
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    LOG.info("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TABLE).listener(listener);

        //
        // step 1: create a single Connection and a BufferedMutator, shared by all worker threads.
        //
        try (final Connection conn = ConnectionFactory.createConnection(getConf());
             final BufferedMutator mutator = conn.getBufferedMutator(params)) {
            for (int i = 0; i < TASK_COUNT; i++) {
                Put p = new Put(Bytes.toBytes("user_" + i));
                p.addColumn(FAMILY, Bytes.toBytes("q" + i % 2), Bytes.toBytes("hello_" + i));
                mutator.mutate(p);
                System.out.println(p);
                // do work... maybe you want to call mutator.flush() after many edits to ensure any of
                // this worker's edits are sent before exiting the Callable
            }
        } catch (IOException e) {
            // exception while creating/destroying Connection or BufferedMutator
            LOG.info("exception while creating/destroying Connection or BufferedMutator", e);
        } // BufferedMutator.close() ensures all work is flushed. Could be the custom listener is
        // invoked from here.
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BufferedMutatorExample2(), args);
    }
}
