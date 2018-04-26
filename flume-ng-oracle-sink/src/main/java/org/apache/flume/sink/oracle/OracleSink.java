package org.apache.flume.sink.oracle;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.oracle.client.OracleConnect;
import org.apache.flume.sink.oracle.configuration.OracleSinkConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ljh on 2017/9/21.
 */
public class OracleSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory
            .getLogger(OracleSink.class);
    private static JdbcTemplate tt;
    private static final int size = 100;
    private final CounterGroup counterGroup = new CounterGroup();
    private Connection cn = null;
    private Statement stmt = null;
    private String tableName;
    private String fildname;
    private String type;
    private String separator;
    private String leng;
    private String sql;
    private int batchSize;
    private String url;
    private String userName;
    private String password;
    DruidDataSource source;
    private SinkCounter sinkCounter;
    private boolean isLocal = false;
    OracleOperationData mysqlOperationData = new OracleOperationData();

    @Override
    public void configure(Context context) {
        //预留 mysql更新配置
        url = context.getString(OracleSinkConfigurationConstants.URL);
        userName = context.getString(OracleSinkConfigurationConstants.USER_NAME);//用户名
        password = context.getString(OracleSinkConfigurationConstants.PASSWORD);//密码
        tableName = context.getString(OracleSinkConfigurationConstants.INDEX_NAME);//落地表名
        leng = context.getString(OracleSinkConfigurationConstants.FILDS_LENGTH);//字符长度
        type = context.getString(OracleSinkConfigurationConstants.TYPES);//数据类型
        fildname = context.getString(OracleSinkConfigurationConstants.FILD_NAME);//字段名称
        batchSize = context.getInteger(OracleSinkConfigurationConstants.BATCH_SIZE, size);
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        logger.info("Mysql sink {} started");
        sinkCounter.start();
        try {
            source = OracleConnect.dataSource(url, userName, password);
            tt =new JdbcTemplate(source);
            cn = OracleConnect.getConnection(source, url, userName, password);//获取连接
            mysqlOperationData.startCraeterBase(cn, tableName, type, fildname, leng);//创建表
            logger.info("------------Creater Table Success or Fail------------");
        } catch (Exception ex) {
            ex.printStackTrace();
            sinkCounter.incrementConnectionFailedCount();
            sinkCounter.incrementConnectionClosedCount();
        }
        sql = mysqlOperationData.sqlCreaterDataSentence(tableName,fildname);
        logger.info("-------link successful-------------");
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        List<Object[]> list = new ArrayList<Object[]>();
        try {
            txn.begin();
            int count;
            for (count = 0; count < batchSize; ++count) {
                Event event = channel.take();
                if (event == null) {
                    break;
                }
                String aa = new String(event.getBody());
                String [] str = mysqlOperationData.startOperationData(aa);
                list.add(str);
            }
            if (count <= 0) {
                sinkCounter.incrementBatchEmptyCount();
                counterGroup.incrementAndGet("channel.underflow");
                status = Status.BACKOFF;
            } else {
                if (count < batchSize) {
                    sinkCounter.incrementBatchUnderflowCount();
                    status = Status.BACKOFF;
                } else {
                    sinkCounter.incrementBatchCompleteCount();
                }

                sinkCounter.addToEventDrainAttemptCount(count);
            }
            if(list.size() > 0){
                tt.batchUpdate(sql, list);
            }
            txn.commit();
            sinkCounter.addToEventDrainSuccessCount(count);
            counterGroup.incrementAndGet("transaction.success");
        } catch (Throwable ex) {
            try {
                txn.rollback();
                counterGroup.incrementAndGet("transaction.rollback");//事务回滚
            } catch (Exception ex2) {
                logger.error(
                        "Exception in rollback. Rollback might not have been successful.",
                        ex2);//回滚异常
            }
            if (ex instanceof Error || ex instanceof RuntimeException) {
                logger.error("Failed to commit transaction. Transaction rolled back.",
                        ex);//无法提交事务回滚
                Throwables.propagate(ex);
            } else {
                logger.error("Failed to commit transaction. Transaction rolled back.",
                        ex);
                throw new EventDeliveryException(
                        "Failed to commit transaction. Transaction rolled back.", ex);//无法提交事务回滚
            }
        } finally {
            txn.close();
        }
        return status;
    }

    @Override
    public synchronized void stop() {
        super.stop();
        if(source != null)
            OracleConnect.shutDownDataSource(source);
        if(cn != null)
            OracleConnect.closeCon(cn);
        sinkCounter.stop();
    }
}
