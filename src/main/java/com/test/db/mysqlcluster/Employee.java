package com.test.db.mysqlcluster;

//import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

//@PersistenceCapable(table = "employee")
//@Index(name = "idx_uhash")
public interface Employee {

//	@PrimaryKey
	int getId();

	void setId(int id);

	String getFirst();

	void setFirst(String first);

	String getLast();

	void setLast(String last);
//
//	@Column(name = "municipality")
//	@Index(name = "idx_municipality")
//	String getCity();
//
//	void setCity(String city);
//
//	Date getStarted();
//
//	void setStarted(Date date);
//
//	Date getEnded();
//
//	void setEnded(Date date);
//
//	Integer getDepartment();
//
//	void setDepartment(Integer department);
}