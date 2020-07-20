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
        CustomerV1 customer = customerBuilder.build ();

        System.out.println ("Customer v1 "+customer.toString ());

        // write the data of customer object to a file
        final DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<> (CustomerV1.class);
        try( DataFileWriter<CustomerV1> dataFileWriter = new DataFileWriter<> (datumWriter) ){
            dataFileWriter.create (CustomerV1.SCHEMA$, new File ("customer_v1.avro"));
            dataFileWriter.append (customer);
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

    }
}
