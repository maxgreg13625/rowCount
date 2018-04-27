import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class HBaseOp{
	private String _kerberosPrincipal="";
	private String _kerberosKeytab="";
	private String _hbaseSiteFile="";
	private String _coreSiteFile="";
	private HTable _targetTable;

	public HBaseOp(String kp, String kk, String hsf, String csf, String tableName){
		this._kerberosPrincipal=kp;
		this._kerberosKeytab=kk;
		this._hbaseSiteFile=hsf;
		this._coreSiteFile=csf;

		initHBaseTable(tableName);
	}
	
	private void initHBaseTable(String name){
		try{
			this._targetTable=new HTable(this.getConfiguration(), name);
		}catch(IOException ioe){
			System.out.println("Caught IOException.");
			System.out.println(ioe.getMessage());
			System.exit(-1);
		}
	}

	private String getKerberosPrincipal(){
		return this._kerberosPrincipal;
	}
	
	private String getKerberosKeytab(){
		return this._kerberosKeytab;
	}
	
	private String getHBaseSiteFile(){
		return this._hbaseSiteFile;
	}

	private String getCoreSiteFile(){
		return this._coreSiteFile;
	}

	private Configuration getConfiguration(){
		Configuration conf=null;

		try{
			conf=HBaseConfiguration.create();
			conf.addResource(new Path(this.getHBaseSiteFile()));
			conf.addResource(new Path(this.getCoreSiteFile()));
	
			conf.setLong("hbase.client.scanner.timeout.period", 240000L);
			conf.setLong("hbase.rpc.timeout", 240000L);
			conf.setStrings("hbase.client.ipc.pool.type", new String[]{"RoundRobinPool"});
			conf.setLong("hbase.client.ipc.pool.size", 20L);
			if(null!=this.getKerberosKeytab() && !this.getKerberosKeytab().isEmpty() 
				|| null!=this.getKerberosPrincipal() && !this.getKerberosPrincipal().isEmpty()){
				conf.set("hbase.productInfoHBaseQuery.keytab", this.getKerberosKeytab());
				conf.set("hbase.productInfoHBaseQuery.principal", this.getKerberosPrincipal());
				UserGroupInformation.setConfiguration(conf);
				User.login(conf, "hbase.productInfoHBaseQuery.keytab", "hbase.productInfoHBaseQuery.principal", InetAddress.getLocalHost().getHostName());
			}
		}catch(UnknownHostException uhe){
			System.out.println("Caught Unknown Host Exception.");
			System.out.println(uhe.getMessage());
			System.exit(-1);
		}catch(IOException ioe){
			System.out.println("Caught IOException.");
			System.out.println(ioe.getMessage());
			System.exit(-1);
		}

		return conf;
	}
	
	public List<String> getScanResult(String startKey, String endKey, String columnFamily, String columnQualifier){
		ResultScanner resultScanner=null;
		List<String> resultStringList=new ArrayList();

		try{
			Scan scan=new Scan();

			if(startKey!="" && endKey!=""){
				scan.setStartRow(Bytes.toBytesBinary(startKey));
				scan.setStopRow(Bytes.toBytesBinary(endKey));
				scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier));
				resultScanner=this._targetTable.getScanner(scan);
			}else{
				//hbase(main):001:0> f_keyonly=org.apache.hadoop.hbase.filter.KeyOnlyFilter.new();
				//hbase(main):002:0* f_firstkey=org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter.new();
				//hbase(main):003:0* flist=org.apache.hadoop.hbase.filter.FilterList.new([f_keyonly, f_firstkey]);
				//hbase(main):004:0* scan 'tableName', {STARTROW=>'startKey',ENDROW=>'endKey',FILTER=>flist}

				//a filter that will only return the first KV from each row.
				scan.setFilter(new FirstKeyOnlyFilter());
			}
		}catch(IOException ioe){
			System.out.println("Caught IOException.");
			System.out.println(ioe.getMessage());
			System.exit(-1);
		}

		for(Result r:resultScanner){
			for(KeyValue kv:r.list()){
				resultStringList.add(Bytes.toString(kv.getValue()));
			}
		}

		return resultStringList;
	}
	
	public String getGetResult(String rowKey, String columnFamily, String columnQualifier){
		String stringResult="";

		try{
			Get get=new Get(Bytes.toBytes(rowKey));

			get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier));
			Result result=this._targetTable.get(get);
			for (KeyValue kv : result.list()){
				stringResult=Bytes.toString(kv.getValue());
			}
		}catch(IOException e){
			e.printStackTrace();
		}
		
		return stringResult;
	}

	public void putPutResult(String rowKey, String columnFamily, String columnQualifier, String value){
		try{
			Put put=new Put(Bytes.toBytes(rowKey));

			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes(value));
			this._targetTable.put(put);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}
