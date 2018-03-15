package hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class hbase_test {
	public static void main(String[] args) throws IOException,
		MasterNotRunningException,ZooKeeperConnectionException{
		// 取得一个数据库连接的配置参数对象
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();

        // 设置连接参数：HBase数据库所在的主机IP
        conf.set("hbase.zookeeper.quorum", "192.168.20.59,192.168.20.61,192.168.20.63");
        System.out.println("---------------连接1-----------------");
        // 设置连接参数：HBase数据库使用的端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        System.out.println("---------------连接2-----------------");
        //conf.set("hbase.master", "192.168.20.59:60000");

        // 取得一个数据库连接对象
        //connection = ConnectionFactory.createConnection(conf);
        //System.out.println("---------------连接3-----------------");
		
		try{
			HBaseAdmin.checkHBaseAvailable(conf);
			System.out.println("connecten made!");
		} catch (Exception error){
			System.err.println("Error connecting HBase:"+error);
			System.exit(1);
		}
	}
}
