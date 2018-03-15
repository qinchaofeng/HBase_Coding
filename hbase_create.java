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

    // ��HBase���ݿ�����Ӷ���
    static Connection connection;

    // ���ݿ�Ԫ���ݲ�������
    static Admin admin;

    @Before
    public static void main(String[] args) throws IOException,
	MasterNotRunningException,ZooKeeperConnectionException {

        // ȡ��һ�����ݿ����ӵ����ò�������
        Configuration conf = HBaseConfiguration.create();

        // �������Ӳ�����HBase���ݿ����ڵ�����IP
        conf.set("hbase.zookeeper.quorum", "192.168.20.59,192.168.20.61,192.168.20.63");
        //conf.set("hbase.zookeeper.quorum", "ly1f-r021701-vm05.local,ly1f-r021701-vm06.local,ly1f-r021701-vm07.local");
        System.out.println("---------------����1-----------------");
        // �������Ӳ�����HBase���ݿ�ʹ�õĶ˿�
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        System.out.println("---------------����2-----------------");
        conf.set("hbase.master", "192.168.20.59:60000");
        //conf.set("zookeeper.znode.parent","/hbase");  
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        
        

        // ȡ��һ�����ݿ����Ӷ���
        connection = ConnectionFactory.createConnection(conf);
        System.out.println("---------------����3-----------------");

        // ȡ��һ�����ݿ�Ԫ���ݲ�������
        admin = connection.getAdmin();
        System.out.println("---------------����4-----------------");
        
        System.out.println("---------------������ START-----------------");

        // ���ݱ����
        String tableNameString = "t_test_qcf11";

        // �½�һ�����ݱ��������
        TableName tableName = TableName.valueOf(tableNameString);
        System.out.println("tablename:"+tableName);
        System.out.println("---------------������ STARTing-----------------");

        // �����Ҫ�½��ı��Ѿ�����
        if(admin.tableExists(tableName)){

            System.out.println("���Ѿ����ڣ�");
        }
        // �����Ҫ�½��ı�����
        else{

            // ���ݱ���������
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            // ������������
            HColumnDescriptor family= new HColumnDescriptor("base");
            System.out.println("---------------������ ing-----------------");

            // �����ݱ����½�һ������
            hTableDescriptor.addFamily(family);

            // �½����ݱ�
            admin.createTable(hTableDescriptor);
        }
        System.out.println("---------------������ END-----------------");
    }
 

}