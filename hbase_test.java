package hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class hbase_test {
	public static void main(String[] args) throws IOException,
		MasterNotRunningException,ZooKeeperConnectionException{
		// ȡ��һ�����ݿ����ӵ����ò�������
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();

        // �������Ӳ�����HBase���ݿ����ڵ�����IP
        conf.set("hbase.zookeeper.quorum", "192.168.20.59,192.168.20.61,192.168.20.63");
        System.out.println("---------------����1-----------------");
        // �������Ӳ�����HBase���ݿ�ʹ�õĶ˿�
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        System.out.println("---------------����2-----------------");
        //conf.set("hbase.master", "192.168.20.59:60000");

        // ȡ��һ�����ݿ����Ӷ���
        //connection = ConnectionFactory.createConnection(conf);
        //System.out.println("---------------����3-----------------");
		
		try{
			HBaseAdmin.checkHBaseAvailable(conf);
			System.out.println("connecten made!");
		} catch (Exception error){
			System.err.println("Error connecting HBase:"+error);
			System.exit(1);
		}
	}
}
