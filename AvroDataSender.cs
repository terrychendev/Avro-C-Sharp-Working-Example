using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.IO;
using System.Net;

using Avro;
using Avro.IO;
using Avro.File;
using Avro.Generic;
using Avro.Specific;


namespace Utility
{
    class AvroDataSender
    {
        /**
		This is an example function that creates an Avro schema object based on pre-defined Avro JSON-like schema. It then
		adds some example data into the record fields, creates a buffer  associated with the record, and sends the buffer 
		out through UDP connection.
        **/
        public static void sender()
        {
            /**
            Creating a schema object by loading schema object file (.avsc)
            Example.AVSC looks like this:

			{"namespace": "example.avro",
			 "type": "record",
			 "name": "User",
			 "fields": [
			     {"name": "name", "type": "string"},
			     {"name": "favorite_number",  "type": ["int", "null"]},
			     {"name": "favorite_color", "type": ["string", "null"]}
			 ]
			}
            **/
            var schema = RecordSchema.Parse(File.ReadAllText(@"C:\Users\user\src\example.avsc")) as RecordSchema;

            //Passing in schema object to get a record object
            var exampleRecorder = new GenericRecord(schema);

            //Filling out records with the corresponding schema
            exampleRecorder.Add("name", "myExample");
            exampleRecorder.Add("favorite_number", 999);
            exampleRecorder.Add("favorite_color", "read");
            
            //Creating an Avro buffer stream
            ByteBufferOutputStream buffer = new ByteBufferOutputStream();

            //Wraping the buffer stream with the encoder that does low level serialization
            Avro.IO.Encoder encoder = new BinaryEncoder(buffer);

            //Creating a writer with the corresponding schema object
            var writer = new DefaultWriter(schema);

            //Write (serialize) record object into buffer outputStream with encoder
            writer.Write<GenericRecord>(exampleRecorder, encoder);

 			//And flush
            buffer.Flush();

            //Creating a UDP client
            UdpClient udpClient = new UdpClient(0);

            //Connect to endpoint with host and port number arguments
            udpClient.Connect("my_udp_end_point.com", 9999);

            //Get buffer list from buffer stream
            List<MemoryStream> bufferList = buffer.GetBufferList();

            //For each memory stream, creating a byte array, and deliver the byte array to endpoint
            //You actually do not need a foreach loop, because you will only have one memory stream
            foreach (MemoryStream ms in bufferList)
            {
                byte[] bufferArray;
                bufferArray = ms.ToArray();
                udpClient.Send(bufferArray, bufferArray.Length);
            }
            udpClient.Close();
        }
    }
}