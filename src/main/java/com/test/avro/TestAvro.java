package com.test.avro;

import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class TestAvro {

	public static void main(String[] args) throws Exception {
		// test1();
		test2();
	}

	private static void test1() throws Exception {
		// TODO Auto-generated method stub
		User user1 = new User();
		user1.setName("Alyssa");
		user1.setFavoriteNumber(256);
		// Leave favorite color null

		// Alternate constructor
		User user2 = new User("Ben", 7, "red");

		// Construct via builder
		User user3 = User.newBuilder().setName("Charlie")
				.setFavoriteColor("blue").setFavoriteNumber(null).build();

		File file = new File("D:/temp/users.avro");
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(
				User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(
				userDatumWriter);
		dataFileWriter.create(user1.getSchema(), file);
		dataFileWriter.append(user1);
		dataFileWriter.append(user2);
		dataFileWriter.append(user3);

		dataFileWriter.close();

		// Deserialize Users from disk
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(
				User.class);
		DataFileReader<User> dataFileReader = new DataFileReader<User>(file,
				userDatumReader);
		User user = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			user = dataFileReader.next(user);
			System.out.println(user);
		}
	}

	private static void test2() throws Exception {
		// TODO Auto-generated method stub
		Schema schema = new Schema.Parser().parse(new File(
				"D:/jhh/tools/test_java/src/user.avsc"));

		GenericRecord user1 = new GenericData.Record(schema);
		user1.put("name", "A");
		user1.put("value", 16);
		// Leave favorite color null

		GenericRecord user2 = new GenericData.Record(schema);
		user2.put("name", "B");
		user2.put("value", 32);

		GenericRecord user3 = new GenericData.Record(schema);
		user3.put("name", "C");
		user3.put("value", 48);

		File file = new File("D:/temp/users.avro");

		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
				schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
				datumWriter);
		dataFileWriter.create(schema, file);
		dataFileWriter.flush();
		dataFileWriter.append(user1);
		dataFileWriter.flush();
		dataFileWriter.append(user2);
		dataFileWriter.flush();
		dataFileWriter.append(user3);
		dataFileWriter.flush();

		dataFileWriter.close();

		// Deserialize users from disk
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
				schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
				file, datumReader);
		GenericRecord user = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			user = dataFileReader.next();
			System.out.println(user);
		}
	}
}
