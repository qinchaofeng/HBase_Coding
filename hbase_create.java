package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Before;

public class hbase_create {

    // 与HBase数据库的连接对象
    static Connection connection;

    // 数据库元数据操作对象
    static Admin admin;

    @Before
    public static void main(String[] args) throws IOException,
	MasterNotRunningException,ZooKeeperConnectionException {

        // 取得一个数据库连接的配置参数对象
        Configuration conf = HBaseConfiguration.create();

        // 设置连接参数：HBase数据库所在的主机IP
        conf.set("hbase.zookeeper.quorum", "172.1.1.1,192.1.1.1,192.1.1.2");
        //conf.set("hbase.zookeeper.quorum", "ly1f-r021701-vm05.local,ly1f-r021701-vm06.local,ly1f-r021701-vm07.local");
        System.out.println("---------------连接1-----------------");
        // 设置连接参数：HBase数据库使用的端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        System.out.println("---------------连接2-----------------");
        conf.set("hbase.master", "192.1.10.1:60000");
        //conf.set("zookeeper.znode.parent","/hbase");  
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        
        

        // 取得一个数据库连接对象
        connection = ConnectionFactory.createConnection(conf);
        System.out.println("---------------连接3-----------------");

        // 取得一个数据库元数据操作对象
        admin = connection.getAdmin();
        System.out.println("---------------连接4-----------------");
        
        System.out.println("---------------创建表 START-----------------");

        // 数据表表名
        String tableNameString = "t_test_qcf11";

        // 新建一个数据表表名对象
        TableName tableName = TableName.valueOf(tableNameString);
        System.out.println("tablename:"+tableName);
        System.out.println("---------------创建表 STARTing-----------------");

        // 如果需要新建的表已经存在
        if(admin.tableExists(tableName)){

            System.out.println("表已经存在！");
        }
        // 如果需要新建的表不存在
        else{

            // 数据表描述对象
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            // 列族描述对象
            HColumnDescriptor family= new HColumnDescriptor("base");
            System.out.println("---------------创建表 ing-----------------");

            // 在数据表中新建一个列族
            hTableDescriptor.addFamily(family);

            // 新建数据表
            admin.createTable(hTableDescriptor);
        }
        System.out.println("---------------创建表 END-----------------");
    }
 

}
