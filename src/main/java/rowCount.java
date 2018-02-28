import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class rowCount{
	private static String _kerberosPrincipal="";
	private static String _kerberosKeytab="";
	private static String _hbaseSiteFile="";
	private static String _coreSiteFile="";
  
	public static Configuration getConfiguration(){
		Configuration conf=null;
		
		try{
			conf=HBaseConfiguration.create();
			conf.addResource(new Path(_hbaseSiteFile));
			conf.addResource(new Path(_coreSiteFile));
	
			conf.setLong("hbase.client.scanner.timeout.period", 240000L);
			conf.setLong("hbase.rpc.timeout", 240000L);
			conf.setStrings("hbase.client.ipc.pool.type", new String[]{"RoundRobinPool"});
			conf.setLong("hbase.client.ipc.pool.size", 20L);
			if(null != _kerberosKeytab && !_kerberosKeytab.isEmpty() || null != _kerberosPrincipal && !_kerberosPrincipal.isEmpty()){
				conf.set("hbase.productInfoHBaseQuery.keytab", _kerberosKeytab);
				conf.set("hbase.productInfoHBaseQuery.principal", _kerberosPrincipal);
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
	
	public static void readConfiguration(){
		Properties properties=new Properties();
		try{
			properties.load(new FileInputStream("app.properties"));
			
			_kerberosPrincipal=properties.getProperty("KerberosPrincipal");
			_kerberosKeytab=properties.getProperty("KerberosKeytab");
			_hbaseSiteFile=properties.getProperty("HBaseSiteFile");
			_coreSiteFile=properties.getProperty("CoreSiteFile");
		}catch(IOException e){
			System.out.println("Open app.properties fail...");
			System.out.println(e.getMessage());
			System.exit(-1);
		}
	}

	public static void printArgs(String[] args){
		System.out.println(args.length);
		for(String s:args)
			System.out.print(String.format("%s ", s));
		System.out.println();
	}
	
	public static void main(String[] args) throws Exception{
		String tableName="";
		String startKey="";
		String endKey="";
		String column="";		

		if(args.length!=4 && args.length!=3){
			printArgs(args);
			System.err.println("Usage: <tableName> <startKey> <endKey> [<columnFamily:column>]");
            System.exit(1);
        }else{
			tableName=args[0];
			startKey=args[1];
			endKey=args[2];
			if(args.length==4){
				column=args[3];
			}
		}

		readConfiguration();
		rowCount(tableName, startKey, endKey, column);
	}

	public static long rowCount(String tableName, String startKey, String endKey, String column){
		long rowCount=0;

		try {
			if(startKey!="" && endKey!=""){
				//set target table
				HTable table=new HTable(getConfiguration(), tableName);

				//set scan criteria
				Scan scan=new Scan();
				scan.setStartRow(Bytes.toBytesBinary(startKey));
				scan.setStopRow(Bytes.toBytesBinary(endKey));
				if(column!=""){
					//hbase(main):001:0> scan 'tableName', {STARTROW=>'startKey',ENDROW=>'endKey',COLUMNS=>'column'}
					String[] columnDetail=column.split(":");
					scan.addColumn(Bytes.toBytes(columnDetail[0]), Bytes.toBytes(columnDetail[1]));
				}else{
					//hbase(main):001:0> f_keyonly=org.apache.hadoop.hbase.filter.KeyOnlyFilter.new();
					//hbase(main):002:0* f_firstkey=org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter.new();
					//hbase(main):003:0* flist=org.apache.hadoop.hbase.filter.FilterList.new([f_keyonly, f_firstkey]);
					//hbase(main):004:0* scan 'tableName', {STARTROW=>'startKey',ENDROW=>'endKey',FILTER=>flist}

					//a filter that will only return the first KV from each row.
					scan.setFilter(new FirstKeyOnlyFilter());
				}

				//start scan
				String startTime=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date());
				ResultScanner resultScanner=table.getScanner(scan);
				//calculate result
				for(Result result : resultScanner){
					rowCount+=result.size();
				}
				String endTime=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date());

				//print result
				System.out.println(String.format("Start Time: %s", startTime));
				System.out.println(String.format("Row Count for %s %s between %s and %s: %d", 
					tableName, column, startKey, endKey, rowCount));
				System.out.println(String.format("Start Time: %s", endTime));
			}
		}catch (IOException e){
			System.out.println(e.getMessage());
	    }
		return rowCount;
	}
}