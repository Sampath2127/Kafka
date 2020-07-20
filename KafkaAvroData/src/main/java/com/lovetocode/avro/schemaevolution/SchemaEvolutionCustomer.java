package com.lovetocode.avro.schemaevolution;

import com.lovetocode.schema.Customer;
import com.lovetocode.schema.CustomerV1;
import com.lovetocode.schema.CustomerV2;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SchemaEvolutionCustomer {

    public static void main( String[] args ) {

        // create a specific record object from schema v1
        CustomerV1.Builder customerBuilder = CustomerV1.newBuilder ();
        customerBuilder.setAge (25);
        customerBuilder.setFirstName ("Sam");
        customerBuilder.setLastName ("Kom");
        customerBuilder.setHeight (178f);
        customerBuilder.setWeight (75.00f);
        customerBuilder.setAutomatedEmail (false);
        CustomerV1 customerV1 = customerBuilder.build ();

        System.out.println ("Customer v1 "+customerV1.toString ());

        // write the data of customer object to a file
        final DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<> (CustomerV1.class);
        try( DataFileWriter<CustomerV1> dataFileWriter = new DataFileWriter<> (datumWriter) ){
            dataFileWriter.create (CustomerV1.SCHEMA$, new File ("customer_v1.avro"));
            dataFileWriter.append (customerV1);
        } catch ( IOException e ) {
            System.out.println ("Error while writing data to file..!");
            e.printStackTrace ();
        }


        //read data from avro schema file using new schema v2
        File file = new File ("customer_v1.avro");
        final DatumReader<CustomerV2> datumReader = new SpecificDatumReader<> (CustomerV2.class);
        try( DataFileReader<CustomerV2> dataFileReader = new DataFileReader<> (file, datumReader) ){
            while ( dataFileReader.hasNext () ){
                CustomerV2 readCustomer = dataFileReader.next ();
                System.out.println ("Customer v2 "+readCustomer.toString ());
//                System.out.println (customer.getFirstName ());
//                System.out.println (customer.getHeight ());
            }
        } catch ( IOException e ) {
            System.out.println ("Unable to read data from Schema object..!");
            e.printStackTrace ();
        }

        // create a specific record object from schema v1
        CustomerV2.Builder customerBuilderv2 = CustomerV2.newBuilder ();
        customerBuilderv2.setAge (25);
        customerBuilderv2.setFirstName ("Sam");
        customerBuilderv2.setLastName ("Kom");
        customerBuilderv2.setHeight (178f);
        customerBuilderv2.setWeight (75.00f);
        customerBuilderv2.setPhoneNo ("123-456-7890");
        customerBuilderv2.setEmail ("customerv2@gmail.com");
        CustomerV2 customerV2 = customerBuilderv2.build ();

        System.out.println ("Customer v2 : "+customerV2.toString ());

        // write the data of customer object to a file
        final DatumWriter<CustomerV2> datumWriterv2 = new SpecificDatumWriter<> (CustomerV2.class);
        try( DataFileWriter<CustomerV2> dataFileWriter = new DataFileWriter<> (datumWriterv2) ){
            dataFileWriter.create (CustomerV2.SCHEMA$, new File ("customer_v2.avro"));
            dataFileWriter.append (customerV2);
        } catch ( IOException e ) {
            System.out.println ("Error while writing data to file..!");
            e.printStackTrace ();
        }


        //read data from avro schema file using new schema v1
        File filev2 = new File ("customer_v2.avro");
        final DatumReader<CustomerV1> datumReaderV2 = new SpecificDatumReader<> (CustomerV1.class);
        try( DataFileReader<CustomerV1> dataFileReader = new DataFileReader<> (filev2, datumReaderV2) ){
            while ( dataFileReader.hasNext () ){
                CustomerV1 readCustomer = dataFileReader.next ();
                System.out.println ("Customer v1 : "+readCustomer.toString ());
//                System.out.println (customer.getFirstName ());
//                System.out.println (customer.getHeight ());
            }
        } catch ( IOException e ) {
            System.out.println ("Unable to read data from Schema object..!");
            e.printStackTrace ();
        }

    }
}
