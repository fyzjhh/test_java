package com.jhh;

import org.apache.commons.cli.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * Main class
 */
public class MysqlDumper {

    public static String COMMA = ",";
    public static String TAB = "	";
    public static String COLON = ":";
    public static String SPACE = " ";
    public static String POINT = ".";
    public static String EMPTY = "";
    public static String UNDERLINE = "_";
    public static String NEWLINE = "\n";
    public static String WAVY = "~";
    public static String SEMI = ";";
    public static String S_QUOTE = "'";
    public static String D_QUOTE = "\"";

    String host = EMPTY;
    String port = EMPTY;
    String user = EMPTY;
    String password = EMPTY;
    String databases = EMPTY;
    String tables = EMPTY;


    Connection conn = null;
    DateFormat datetimeformat = new SimpleDateFormat(
            "yyyy-MM-dd_HH:mm:ss");
    DateFormat spacedatetimeformat = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");
    DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");

    DataObject table_dataobject = null;

    String datadir = "/tmp/";
    String query = EMPTY;
    String p1, p2, p3 = EMPTY;
    String str1, str2, str3 = EMPTY;
    DataObject tmp_dataobject = null;


    public static void main(String[] args) throws Exception {


        String tmp_args = " -h 10.199.139.39  -P 3307 -u oec -p oec_123 -d oec -t oec_consume_handle_exception_log,oec_consume_handle_pts_extend_record,oec_lease_empower_detail_holder_relation " ;
        args = tmp_args.trim().split(" +");

        MysqlDumper mysqlDumper = new MysqlDumper();

        mysqlDumper.parseOptions(args);

        mysqlDumper.createConn();

        mysqlDumper.getDumperTableInfo();

        mysqlDumper.execute();
    }

    private void execute() throws Exception {
             /*
         1  <= 10G
         2  >  10G
          */

        long yuzhi = 10 * 1024 * 1048576;
        ExecutorService exec = Executors.newCachedThreadPool();
        HashMap taskMap = new HashMap<String, Future>();


        for (int i = 0; i < table_dataobject.row_list.size(); i++) {
            RowObject tmp_ro = table_dataobject.row_list.get(i);
            List<String> key_list = tmp_ro.getKey_list();
            str1 = concatList(key_list, POINT, EMPTY);
            List data_list = tmp_ro.getData_list();
            Long table_rows = (Long) data_list.get(3);
            Long avg_row_length = (Long) data_list.get(4);
            Long table_size = table_rows * avg_row_length;
            int thread_count = Math.round(table_size / yuzhi);


            if (thread_count <= 1) {
                String sql = "SELECT * FROM <in_table> ".replaceAll("<in_table>", str1);
                WorkerThread taski = new WorkerThread(str1, -1, sql);
                Future futurei = exec.submit(taski);
                taskMap.put(i, futurei);
            } else {

                String base_id_sql = "select  min(<shuffle_field>) minid , max(<shuffle_field>) maxid from <table> "
                        .replaceAll("<shuffle_field>", "id");
                tmp_dataobject = getDataObject(base_id_sql, EMPTY);
                RowObject tmp_rowobject = table_dataobject.row_list.get(0);

                List tmp_data_list = tmp_ro.getData_list();
                Long minid = (Long) tmp_data_list.get(0);
                Long maxid = (Long) tmp_data_list.get(1) + 1;

                Long step = (maxid - minid) / thread_count;
                for (int j = 0; j < thread_count; j++) {
                    Long start_value = minid + step * j;
                    Long stop_value = minid + step * (j + 1);

                    String sql = "SELECT * FROM <in_table> where <shuffle_field> >= <start_value> AND <shuffle_field> < <stop_value>"
                            .replaceAll("<start_value>", String.valueOf(start_value))
                            .replaceAll("<stop_value>", String.valueOf(stop_value))
                            .replaceAll("<shuffle_field>", "id");
                    WorkerThread taski = new WorkerThread(str1, j, sql);
                    Future futurei = exec.submit(taski);
                    taskMap.put(i, futurei);
                }
            }

        }


        Iterator iter = taskMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            Object key = entry.getKey();
            Future val = (Future) entry.getValue();
            Object ret = (Object) val.get();
            System.out.println(key + "result is " + ret);
        }

        exec.shutdown();
    }

    private void getDumperTableInfo() throws SQLException {

        if (databases == null) {
            query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name not in ('mysql','sys','information_schema','performance_schema','test')";
            DataObject db_do = getDataObject(query, "0");

            List<String> db_list = new ArrayList<String>();
            for (int i = 0; i < db_do.row_list.size(); i++) {
                RowObject tmp_ro = db_do.row_list.get(i);
                List<String> key_list = tmp_ro.getKey_list();
                db_list.add(concatList(key_list, EMPTY, EMPTY));
            }
            str1 = concatList(db_list, COMMA, S_QUOTE);
            query = "SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_ROWS,AVG_ROW_LENGTH FROM information_schema.TABLES WHERE TABLE_TYPE='BASE TABLE' and TABLE_SCHEMA in ( <in_databases> )".replaceAll("<in_databases>", str1);
            table_dataobject = getDataObject(query, "0,1");
        } else {
            if (databases.contains(",")) {
                String[] database_array = databases.split(" *, *");
                List<String> database_list = new ArrayList<String>(Arrays.asList(database_array));

                str1 = concatList(database_list, COMMA, S_QUOTE);
                query = "SELECT TABLE_SCHEMA,TABLE_SCHEMA,TABLE_NAME,TABLE_ROWS,AVG_ROW_LENGTH FROM information_schema.TABLES WHERE TABLE_TYPE='BASE TABLE' and TABLE_SCHEMA in ( <in_databases> )".replaceAll("<in_databases>", str1);
                table_dataobject = getDataObject(query, "0,1");
            } else {
                if (tables == null) {
                    query = "SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_ROWS,AVG_ROW_LENGTH FROM information_schema.TABLES WHERE TABLE_TYPE='BASE TABLE' and TABLE_SCHEMA = '<in_databases>'".replaceAll("<in_databases>", databases);
                    table_dataobject = getDataObject(query, "0,1");

                } else {
                    String[] table_array = tables.split(" *, *");
                    List<String> table_list = new ArrayList<String>(Arrays.asList(table_array));

                    str1 = concatList(table_list, COMMA, S_QUOTE);
                    query = "SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_ROWS,AVG_ROW_LENGTH FROM information_schema.TABLES WHERE TABLE_TYPE='BASE TABLE' and TABLE_SCHEMA = '<in_databases>' and TABLE_NAME in (<in_tables>)".replaceAll("<in_databases>", databases).replaceAll("<in_tables>", str1);
                    table_dataobject = getDataObject(query, "0,1");
                }
            }
        }
    }

    private void createConn() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        String url = "jdbc:mysql://<in_ip>:<in_port>/oec".replaceAll("<in_ip>", host).replaceAll("<in_port>", port);
        conn = DriverManager.getConnection(url, user, password);
    }


    private String concatList(List<String> key_list, String seperator, String quote) {
        String tmp_str = EMPTY;
        for (int i = 0; i < key_list.size(); i++) {
            if (i > 0) {
                tmp_str += seperator;
            }
            tmp_str += quote + key_list.get(i) + quote;

        }
        return tmp_str;
    }

    private void parseOptions(String[] args) {
        Options opts = new Options();
        opts.addOption("H", "help", false, "");
        opts.addOption("h", "host", true, "");
        opts.addOption("P", "port", true, "");
        opts.addOption("u", "user", true, "");
        opts.addOption("p", "password", true, "");
        opts.addOption("d", "databases", true, "");
        opts.addOption("t", "tables", true, "");
        BasicParser parser = new BasicParser();
        CommandLine cl = null;

        try {
            cl = parser.parse(opts, args);
        } catch (ParseException e) {
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp("Options", opts);
            System.exit(0);
        }
        host = cl.getOptionValue("h").replaceAll(D_QUOTE, EMPTY);
        port = cl.getOptionValue("P").replaceAll(D_QUOTE, EMPTY);
        user = cl.getOptionValue("u").replaceAll(D_QUOTE, EMPTY);
        password = cl.getOptionValue("p").replaceAll(D_QUOTE, EMPTY);
        databases = cl.getOptionValue("d").replaceAll(D_QUOTE, EMPTY);
        tables = cl.getOptionValue("t").replaceAll(D_QUOTE, EMPTY);
    }


    private DataObject getDataObject(String sql, String primary_column_str) throws SQLException {

        DataObject ret_do = new DataObject();

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(sql);

        ResultSetMetaData md = rs.getMetaData();
        ret_do.md = md;
        ret_do.primary_column_str = primary_column_str;
        ret_do.sql = sql;

        String[] primary_column_arr = primary_column_str.split(" *, *");
        List<String> primary_column_list = new ArrayList<String>(Arrays.asList(primary_column_arr));

        while (rs.next()) {
            List<String> key_list = new ArrayList<String>();
            List<String> data_list = new ArrayList<String>();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                // add primary key
                if (primary_column_list.contains(i - 1)) {
                    key_list.add(rs.getString(i));
                }
                data_list.add(rs.getString(i));
            }

            RowObject do_ro = new RowObject(key_list, data_list);
            ret_do.row_list.add(do_ro);
        }
        return ret_do;
    }

    private static String repfile(String f) {
        if (f != null) {
            return f.replace("\\", "fxg").replace("/", "xg").replace("?", "wh")
                    .replace("\t", "tab").replace("<", "xyh")
                    .replace(">", "dyh").replace("\"", "syh")
                    .replace("|", "sx").replace("*", "xh").replace(":", "mh");
        } else {
            return null;
        }
    }

    class WorkerThread implements Callable {
        private String tablename;
        private int idx;
        private String sql;

        public WorkerThread(String tablename, int idx, String sql) {
            this.tablename = tablename;
            this.idx = idx;
            this.sql = sql;
        }

        public Object call() throws Exception {
            try {

                Long startTime = System.currentTimeMillis();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql);

                ResultSetMetaData md = rs.getMetaData();


                String absfilename = datadir;
                if (idx >= 0) {
                    absfilename += repfile(tablename + UNDERLINE + UNDERLINE + idx + ".sql");
                } else {
                    absfilename += repfile(tablename + ".sql");
                }
                File dstfile = new File(absfilename);
                FileOutputStream output = new FileOutputStream(dstfile, false);
                OutputStreamWriter osw = new OutputStreamWriter(output);

                Long rows_count = 0L;
                if(rs.next()){
                    osw.write("INSERT INTO " + tablename + " values " + NEWLINE);
                    rows_count = writeLine(rs, md, osw, rows_count);

                }

                while (rs.next()) {

                    rows_count = writeLine(rs, md, osw, rows_count);

                    if (rows_count % 10000 == 9999) {
                        System.out.println(datetimeformat.format(new Date()) + rows_count + " rows writed ! ");
                    }
                }

                if (rows_count % 10000 != 9999) {
                    System.out.println(datetimeformat.format(new Date()) + rows_count + " rows writed ! ");
                }

                osw.write("; " + NEWLINE);

                rs.close();
                st.close();
                output.close();

                Long endTime = System.currentTimeMillis();
                System.out.println(tablename + COMMA + idx + " spent time " + (endTime - startTime));
                return 0;

            } catch (Exception e) {
                e.printStackTrace();
                return 1;
            }
        }

    }

    private Long writeLine(ResultSet rs, ResultSetMetaData md, OutputStreamWriter osw, Long rows_count) throws SQLException, IOException {
        String tmpstr = EMPTY;
        for (int i = 1; i <= md.getColumnCount(); i++) {
            md.getColumnType(i);
            md.getColumnTypeName(i);

            tmpstr += rs.getObject(i).toString();
            if (i > 1) {
                tmpstr += COMMA;
            }
        }
        rows_count++;
        osw.write("(" + tmpstr + ")," + NEWLINE);
        return rows_count;
    }
}



