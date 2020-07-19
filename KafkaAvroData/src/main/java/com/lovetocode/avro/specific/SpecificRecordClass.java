package com.lovetocode.avro.specific;

import com.lovetocode.schema.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.File;
import java.io.IOException;

public class SpecificRecordClass {

    public static void main( String[] args ) {

        // create a specific record object from schema
        Customer.Builder customerBuilder = Customer.newBuilder ();
        customerBuilder.setAge (25);
        customerBuilder.setFirstName ("Sam");
        customerBuilder.setLastName ("Kom");
        customerBuilder.setHeight (178f);
        customerBuilder.setWeight (75.00f);
        customerBuilder.setAutomatedEmail (false);
        Customer customer = customerBuilder.build ();

        System.out.println (customer);

        // write the data of customer object to a file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<> (Customer.class);
        try( DataFileWriter<Customer> dataFileWriter = new DataFileWriter<> (datumWriter) ){
            dataFileWriter.create (Customer.SCHEMA$, new File ("customer-specific.avro"));
            dataFileWriter.append (customer);
        } catch ( IOException e ) {
            System.out.println ("Error while writing data to file..!");
            e.printStackTrace ();
        }


        //read data from avro schema file
        File file = new File ("customer-specific.avro");
        final DatumReader<Customer> datumReader = new SpecificDatumReader<> (Customer.class);
        try( DataFileReader<Customer> dataFileReader = new DataFileReader<> (file, datumReader) ){
            while ( dataFileReader.hasNext () ){
                Customer readCustomer = dataFileReader.next ();
                System.out.println (customer.getFirstName ());
                System.out.println (customer.getHeight ());
            }
        } catch ( IOException e ) {
            System.out.println ("Unable to read data from Schema object..!");
            e.printStackTrace ();
        }

    }
}
