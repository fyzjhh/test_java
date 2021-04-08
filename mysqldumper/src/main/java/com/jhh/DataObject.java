package com.test.other.mysqldumper;

import java.sql.*;
import java.util.*;

/**
 * Main class
 */
public class DataObject {

    List<RowObject> row_list = new ArrayList<RowObject>();

    ResultSetMetaData md = null;
    Long record_limit = 10000L;
    String sql = "";
    String primary_column_str = "";


}


