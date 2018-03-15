package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;	
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class hbase_demo {

    // ��HBase���ݿ�����Ӷ���
    Connection connection;

    // ���ݿ�Ԫ���ݲ�������
    Admin admin;

    @Before
    public void setUp() throws Exception {

        // ȡ��һ�����ݿ����ӵ����ò�������
        Configuration conf = HBaseConfiguration.create();

        // �������Ӳ�����HBase���ݿ����ڵ�����IP
        conf.set("hbase.zookeeper.quorum", "192.168.20.59,192.168.20.61,192.168.20.63");
        System.out.println("---------------����1-----------------");
        // �������Ӳ�����HBase���ݿ�ʹ�õĶ˿�
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        System.out.println("---------------����2-----------------");

        // ȡ��һ�����ݿ����Ӷ���
        connection = ConnectionFactory.createConnection(conf);
        System.out.println("---------------����3-----------------");

        // ȡ��һ�����ݿ�Ԫ���ݲ�������
        admin = connection.getAdmin();
        System.out.println("---------------����4-----------------");
    }

    /**
     * ������
     */
    public void createTable() throws IOException{

        System.out.println("---------------������ START-----------------");

        // ���ݱ����
        String tableNameString = "t_book";

        // �½�һ�����ݱ��������
        TableName tableName = TableName.valueOf(tableNameString);

        // �����Ҫ�½��ı��Ѿ�����
        if(admin.tableExists(tableName)){

            System.out.println("���Ѿ����ڣ�");
        }
        // �����Ҫ�½��ı�����
        else{

            // ���ݱ���������
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

            // ������������
            HColumnDescriptor family= new HColumnDescriptor("base");;

            // �����ݱ����½�һ������
            hTableDescriptor.addFamily(family);

            // �½����ݱ�
            admin.createTable(hTableDescriptor);
        }

        System.out.println("---------------������ END-----------------");
    }
    /**
     * ��ѯ��������
     */
     @Test
     public void queryTable() throws IOException{

        System.out.println("---------------��ѯ�������� START-----------------");

        // ȡ�����ݱ����
        Table table = connection.getTable(TableName.valueOf("t_book")); //t_book

        // ȡ�ñ�����������
        ResultScanner scanner = table.getScanner(new Scan());

        // ѭ��������е�����
        for (Result result : scanner) {

            byte[] row = result.getRow();
            System.out.println("row key is:" + new String(row));

            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells) {

                byte[] familyArray = cell.getFamilyArray();
                byte[] qualifierArray = cell.getQualifierArray();
                byte[] valueArray = cell.getValueArray();

                System.out.println("row value is:" + new String(familyArray) + new String(qualifierArray) 
                                                                             + new String(valueArray));
            }
        }

        System.out.println("---------------��ѯ�������� END-----------------");

    }

    /**
     * ���м���ѯ������
     */
     @Test
    public void queryTableByRowKey() throws IOException{

        System.out.println("---------------���м���ѯ������ START-----------------");

        // ȡ�����ݱ����
        Table table = connection.getTable(TableName.valueOf("t_book"));

        // �½�һ����ѯ������Ϊ��ѯ����
        Get get = new Get("row8".getBytes());

        // ���м���ѯ����
        Result result = table.get(get);

        byte[] row = result.getRow();
        System.out.println("row key is:" + new String(row));

        List<Cell> listCells = result.listCells();
        for (Cell cell : listCells) {

            byte[] familyArray = cell.getFamilyArray();
            byte[] qualifierArray = cell.getQualifierArray();
            byte[] valueArray = cell.getValueArray();

            System.out.println("row value is:" + new String(familyArray) + new String(qualifierArray) 
                                                                         + new String(valueArray));
        }

        System.out.println("---------------���м���ѯ������ END-----------------");

    }

    /**
     * ��������ѯ������
     */
     @Test
    public void queryTableByCondition() throws IOException{

        System.out.println("---------------��������ѯ������ START-----------------");

        // ȡ�����ݱ����
        Table table = connection.getTable(TableName.valueOf("t_book"));

        // ����һ����ѯ������
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("base"), Bytes.toBytes("name"), 
                                                    CompareOp.EQUAL, Bytes.toBytes("bookName6"));

        // ����һ�����ݱ�ɨ����
        Scan scan = new Scan();

        // ����ѯ���������뵽���ݱ�ɨ��������
        scan.setFilter(filter);

        // ִ�в�ѯ��������ȡ�ò�ѯ���
        ResultScanner scanner = table.getScanner(scan);

        // ѭ�������ѯ���
        for (Result result : scanner) {
            byte[] row = result.getRow();
            System.out.println("row key is:" + new String(row));

            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells) {

                byte[] familyArray = cell.getFamilyArray();
                byte[] qualifierArray = cell.getQualifierArray();
                byte[] valueArray = cell.getValueArray();

                System.out.println("row value is:" + new String(familyArray) + new String(qualifierArray) 
                                                                             + new String(valueArray));
            }
        }

        System.out.println("---------------��������ѯ������ END-----------------");

    }

    /**
     * ��ձ�
     */
    @Test
    public void truncateTable() throws IOException{

        System.out.println("---------------��ձ� START-----------------");

        // ȡ��Ŀ�����ݱ�ı�������
        TableName tableName = TableName.valueOf("t_book");

        // ���ñ�״̬Ϊ��Ч
        admin.disableTable(tableName);
        // ���ָ���������
        admin.truncateTable(tableName, true);

        System.out.println("---------------��ձ� End-----------------");
    }

    /**
     * ɾ����
     */
    @Test
    public void deleteTable() throws IOException{

        System.out.println("---------------ɾ���� START-----------------");

        // ���ñ�״̬Ϊ��Ч
        admin.disableTable(TableName.valueOf("t_book"));
        // ɾ��ָ�������ݱ�
        admin.deleteTable(TableName.valueOf("t_book"));

        System.out.println("---------------ɾ���� End-----------------");
    }

    /**
     * ɾ����
     */
    @Test
    public void deleteByRowKey() throws IOException{

        System.out.println("---------------ɾ���� START-----------------");

        // ȡ�ô����������ݱ����
        Table table = connection.getTable(TableName.valueOf("t_book"));

        // ����ɾ����������
        Delete delete = new Delete(Bytes.toBytes("row2"));

        // ִ��ɾ������
        table.delete(delete);

        System.out.println("---------------ɾ���� End-----------------");

    }

    /**
     * ɾ���У���������
     */
    @Test
    public void deleteByCondition() throws IOException, DeserializationException{

        System.out.println("---------------ɾ���У��������� START-----------------");

        // ����1������queryTableByCondition()����ȡ����Ҫɾ���������б� 

        // ����2��ѭ������1�Ĳ�ѯ�������ÿ���������deleteByRowKey()����

        System.out.println("---------------ɾ���У��������� End-----------------");

    }

    /**
     * �½�����
     */
    @Test
    public void addColumnFamily() throws IOException{

        System.out.println("---------------�½����� START-----------------");

        // ȡ��Ŀ�����ݱ�ı�������
        TableName tableName = TableName.valueOf("t_book");

        // �����������
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("more");

        // ���´�����������ӵ�ָ�������ݱ�
        admin.addColumn(tableName, columnDescriptor);

        System.out.println("---------------�½����� END-----------------");
    }

    /**
     * ɾ������
     */
    @Test
    public void deleteColumnFamily() throws IOException{

        System.out.println("---------------ɾ������ START-----------------");

        // ȡ��Ŀ�����ݱ�ı�������
        TableName tableName = TableName.valueOf("t_book");

        // ɾ��ָ�����ݱ��е�ָ������
        admin.deleteColumn(tableName, "more".getBytes());

        System.out.println("---------------ɾ������ END-----------------");
    }

    /**
     * ��������
     */
    @Test
    public void insert() throws IOException{

        System.out.println("---------------�������� START-----------------");

        // ȡ��һ�����ݱ����
        Table table = connection.getTable(TableName.valueOf("t_book"));

        // ��Ҫ�������ݿ�����ݼ���
        List<Put> putList = new ArrayList<Put>();

        Put put;

        // �������ݼ���
        for(int i = 0; i < 10; i++){
            put = new Put(Bytes.toBytes("row" + i));
            put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("name"), Bytes.toBytes("bookName" + i));

            putList.add(put);
        }

        // �����ݼ��ϲ��뵽���ݿ�
        table.put(putList);

        System.out.println("---------------�������� END-----------------");
    }
  

}