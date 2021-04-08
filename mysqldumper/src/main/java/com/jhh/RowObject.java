package com.test.other.mysqldumper;

import java.util.List;

public class RowObject {
    List<String> key_list = null;
    List data_list = null;

    public RowObject(List<String> key_list, List data_list) {
        this.key_list = key_list;
        this.data_list = data_list;
    }

    public List<String> getKey_list() {
        return key_list;
    }

    public void setKey_list(List<String> key_list) {
        this.key_list = key_list;
    }

    public List getData_list() {
        return data_list;
    }

    public void setData_list(List data_list) {
        this.data_list = data_list;
    }
}