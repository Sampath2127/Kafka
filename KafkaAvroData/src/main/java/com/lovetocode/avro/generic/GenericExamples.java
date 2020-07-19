package com.lovetocode.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericExamples {

    public static void main( String[] args ) {

        //Define schema
        Schema.Parser parser = new Schema.Parser ();
        Schema schema = parser.parse ("{\n" +
                "     \"type\": \"record\",\n" +
                "     \"namespace\": \"com.example\",\n" +
                "     \"name\": \"Customer\",\n" +
                "     \"doc\": \"Avro Schema for our Customer\",     \n" +
                "     \"fields\": [\n" +
                "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
                "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
                "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
                "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
                "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
                "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n" +
                "     ]\n" +
                "}");
        //create generic record
        GenericRecordBuilder customerBuilder = new GenericRecordBuilder (schema);
        customerBuilder.set ("first_name", "Sam");
        customerBuilder.set ("last_name", "Kom"); // if default values are not set run time exception happens for a field.
        customerBuilder.set ("age", 27);
        customerBuilder.set ("height", 5.10f);
        customerBuilder.set ("weight", 75.00f);
        customerBuilder.set ("automated_email", false); // comment for defaulted values to be assigned.
        GenericData.Record customer = customerBuilder.build ();

        System.out.println (customer);

        //create generic record
        GenericRecordBuilder customerDefaultBuilder = new GenericRecordBuilder (schema);
        customerDefaultBuilder.set ("first_name", "Sam");
        customerDefaultBuilder.set ("last_name", "Kom"); // if default values are not set run time exception happens for a field.
        customerDefaultBuilder.set ("age", 27);
        customerDefaultBuilder.set ("height", 5.10f);
        customerDefaultBuilder.set ("weight", 75.00f);
//        customerBuilder.set ("automated_email", false); // comment for defaulted values to be assigned.
        GenericData.Record customerDefault = customerBuilder.build ();

        System.out.println (customerDefault);

        //write GR to file
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<> (schema);
        try( DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<> (datumWriter)){
            dataFileWriter.create (customer.getSchema (), new File ("customer-generic.avro"));
            dataFileWriter.append (customer);
            dataFileWriter.append (customerDefault);
            System.out.println ("Data written successfully to file ...!");
        } catch ( IOException e ) {
            System.out.println ("couldn't write to file..!");
            e.printStackTrace ();
        }
        //read GR from file
        final File file = new File ("customer-generic.avro");
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<> ();
        GenericRecord customerRecord;
        try( DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord> (file, datumReader)){
           customerRecord =  dataFileReader.next ();

            System.out.println (customerRecord.toString ());

//            System.out.println ("FirstName "+ customerRecord.get ("first_name"));
//
//            System.out.println ("Non-existent field: "+ customerRecord.get ("not_here"));
        } catch ( IOException e ) {
            e.printStackTrace ();
        }

        //interpret as a generic record
    }
}
