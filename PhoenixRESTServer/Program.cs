using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RestSharp;

namespace PhoenixRESTServer
{
    class Program
    {
        static void Main(string[] args)
        {
            // You will need to replace these values with the true values of your PhoenixRESTServer that is running.
            var phoenixRestServer = "192.168.94.130";
            var phoenixRestPort = "9000";

            var client = new RestClient("http://" + phoenixRestServer + ":" + phoenixRestPort);

            //--------------- Create a sample Table "DotNetTest" -------------------
            System.Console.WriteLine("\n");
            var cttRequest = new RestRequest("phoenix/ddl", Method.POST);
            cttRequest.RequestFormat = DataFormat.Json;

            PhoenixRequest model = new PhoenixRequest()
            {
                query = "CREATE TABLE IF NOT EXISTS DotNetTest(id BIGINT not null primary key, order_time TIMESTAMP, order_amt DOUBLE, order_items VARCHAR[], order_id BIGINT, customer_name VARCHAR, customer_phone VARCHAR, retail_ident VARCHAR);"
            };

            cttRequest.AddBody(model);
            IRestResponse response = client.Execute(cttRequest);
            System.Console.WriteLine("Create Table Response: " + response.Content);

            //-- Create the Phoenix Global Secondary indexes on order_id, customer_name, customer_phone, and retail_ident ---
            System.Console.WriteLine("\n");
            var request = new RestRequest("phoenix/ddl", Method.POST);
            request.RequestFormat = DataFormat.Json;

            model = new PhoenixRequest()
            {
                query = "CREATE INDEX IF NOT EXISTS order_id_idx ON DotNetTest(order_id);"
            };

            request.AddBody(model);
            response = client.Execute(request);
            System.Console.WriteLine("Created order_id_idx: " + response.Content);

            //Create customer_name_idx
            System.Console.WriteLine("\n");
            request = new RestRequest("phoenix/ddl", Method.POST);
            request.RequestFormat = DataFormat.Json;

            model = new PhoenixRequest()
            {
                query = "CREATE INDEX IF NOT EXISTS customer_name_idx ON DotNetTest(customer_name);"
            };

            request.AddBody(model);
            response = client.Execute(request);
            System.Console.WriteLine("Created customer_name_idx: " + response.Content);

            //Create customer_phone_idx
            System.Console.WriteLine("\n");
            request = new RestRequest("phoenix/ddl", Method.POST);
            request.RequestFormat = DataFormat.Json;

            model = new PhoenixRequest()
            {
                query = "CREATE INDEX IF NOT EXISTS customer_phone_idx ON DotNetTest(customer_phone);"
            };

            request.AddBody(model);
            response = client.Execute(request);
            System.Console.WriteLine("Created customer_phone_idx: " + response.Content);


            //Create retail_ident_idx
            System.Console.WriteLine("\n");
            request = new RestRequest("phoenix/ddl", Method.POST);
            request.RequestFormat = DataFormat.Json;

            model = new PhoenixRequest()
            {
                query = "CREATE INDEX IF NOT EXISTS retail_ident_idx ON DotNetTest(retail_ident);"
            };

            request.AddBody(model);
            response = client.Execute(request);
            System.Console.WriteLine("Created customer_name_idx: " + response.Content);


            //--------------- Insert a few sample rows -------------------
           
            //Sample customer Jeremy Dyer
            System.Console.WriteLine("\n");
            request = new RestRequest("phoenix/dml", Method.POST);
            request.RequestFormat = DataFormat.Json;
            model = new PhoenixRequest()
            {
                query = "UPSERT INTO DotNetTest VALUES(1, to_date('2015-07-14 12:12:42', 'yyyy-MM-dd HH:mm:ss'), 547.45, ARRAY['Kickboxing Gloves', 'Showa motorcyle helmet', 'Monster Energy Drink'], 123456789, 'Jeremy Dyer', '404-772-0854', 'HortonworksSE');"
            };

            request.AddBody(model);
            response = client.Execute(request);

            //Sample customer Randy Gelhausen
            System.Console.WriteLine("\n");
            request = new RestRequest("phoenix/dml", Method.POST);
            request.RequestFormat = DataFormat.Json;
            model = new PhoenixRequest()
            {
                query = "UPSERT INTO DotNetTest VALUES(2, to_date('2015-07-04 15:34:23', 'yyyy-MM-dd HH:mm:ss'), 35.45, ARRAY['Coldplay Album', 'randerzander Github Subscription', 'Sugar free Redbull'], 1234567810, 'Randy Gelhausen', '404-111-0000', 'HortonworksSE');"
            };

            request.AddBody(model);
            response = client.Execute(request);

            System.Console.WriteLine("Insert sample data: " + response.Content);


            //Query some values by TIMESTAMP order_time. First Query will exclude transaction by equality. Second will include with >=
            System.Console.WriteLine("\n");
            request = new RestRequest("phoenix/dml", Method.GET);
            request.RequestFormat = DataFormat.Json;
            request.AddParameter("query", "SELECT * FROM DotNetTest WHERE order_time > to_date('2015-07-04 15:34:23');");
            response = client.Execute(request);
            System.Console.WriteLine("Select orders greater than '2015-07-04 15:34:23': " + response.Content);

            System.Console.WriteLine("\n");
            request = new RestRequest("phoenix/dml", Method.GET);
            request.RequestFormat = DataFormat.Json;
            request.AddParameter("query", "SELECT * FROM DotNetTest WHERE order_time >= to_date('2015-07-04 15:34:23');");
            response = client.Execute(request);
            System.Console.WriteLine("Select orders greater than '2015-07-04 15:34:23': " + response.Content);


            //Query using the ANY() function to check for an Array type containing ANY value provided. There is also ALL()
            System.Console.WriteLine("\n");
            request = new RestRequest("phoenix/dml", Method.GET);
            request.RequestFormat = DataFormat.Json;
            request.AddParameter("query", "SELECT * FROM DotNetTest WHERE 'Kickboxing Gloves' = ANY(order_items);");
            response = client.Execute(request);
            System.Console.WriteLine("ANY(order_items) = 'Kickboxing Gloves': " + response.Content);


            //Commented out in case you don't want to drop the table.
            //Delete the NCRDotNetTest table from HBase
            // System.Console.WriteLine("\n");
            //request = new RestRequest("phoenix/ddl", Method.DELETE);
            //request.RequestFormat = DataFormat.Json;
            //request.AddParameter("query", "DROP TABLE DotNetTest");
            //response = client.Execute(request);
            //System.Console.WriteLine("Drop table DotNetTest Response: " + response.Content);

            System.Threading.Thread.Sleep(10000);
        }
    }
}
