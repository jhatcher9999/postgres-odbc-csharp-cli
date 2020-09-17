using System;
using System.Data.Odbc;
using System.Text;

namespace postgres_odbc_csharp_cli
{
    class OdbcTester
    {

        private string _connString = "DSN=PostgreSQL;Trusted_Connection=Yes";
        //private string _connString = "Driver={PostgreSQL};Server=localhost;Port=26527;Database=test;Uid=;Pwd=;";
        //private string _connString = "DSN=PostgreSQL;UID='';PWD=''";

        private Random random = new Random((int)DateTime.Now.Ticks);


        public void Run()
        {
            Console.WriteLine("Starting TEST 1: Querying Data");
            Console.WriteLine("=============================================");
            Console.WriteLine();
            QueryData();

            int maxId = GetMaxCustomerID();

            Console.WriteLine("Starting TEST 2: Inserting Data");
            Console.WriteLine("=============================================");
            Console.WriteLine();
            InsertData(maxId + 1);
        }

        private void QueryData()
        {

            OdbcConnection connection = new OdbcConnection(_connString);

            try
            {
                connection.Open();
                // Console.WriteLine("Connection State: " + connection.State.ToString());
                // Console.WriteLine();

                OdbcDataReader reader = null;
                try
                {
                    string query = "SELECT * FROM test.customer ORDER BY customer_id DESC LIMIT 10;";
                    OdbcCommand command = new OdbcCommand(query, connection);

                    reader = command.ExecuteReader();

                    String[] fieldNames = { "Customer ID", "Name" };
                    while(reader.Read() == true) 
                    {
                        Console.WriteLine("Customer");
                        Console.WriteLine("-------------------");
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            Console.Write(fieldNames[i] + ": ");
                            Console.WriteLine(reader.GetString(i));
                        }
                        Console.WriteLine();
                    }
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
                finally
                {
                    if (reader != null)
                    {
                        reader.Close();
                    }
                }
                
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                connection.Close();
                // Console.WriteLine("Connection State: " + connection.State);
                // Console.WriteLine();

            }
        }

        private void InsertData(int customerID)
        {
            string name = GetRandomString(5);
            InsertData(customerID, name);
        }

        private string GetRandomString(int length)
        {
            const string pool = "abcdefghijklmnopqrstuvwxyz";
            var builder = new StringBuilder();

            for (var i = 0; i < length; i++)
            {
                var c = pool[random.Next(0, pool.Length)];
                builder.Append(c);
            }

            return builder.ToString();
        }

        private void InsertData(int customerID, string name)
        {

            OdbcConnection connection = new OdbcConnection(_connString);

            try
            {
                connection.Open();
                // Console.WriteLine("Connection State: " + connection.State.ToString());
                // Console.WriteLine();

                try
                {
                    string query = "INSERT INTO test.customer ( customer_id, name) VALUES ( ?, ? );";
                    OdbcCommand command = new OdbcCommand(query, connection);

                    command.Parameters.Add("@customer_id", OdbcType.Int).Value = customerID;
                    command.Parameters.Add("@name", OdbcType.VarChar).Value = name;

                    Console.WriteLine("Running INSERT query with values " + customerID.ToString() + ", " + name);
                    command.ExecuteNonQuery();

                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
                
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                connection.Close();
                // Console.WriteLine("Connection State: " + connection.State);
                // Console.WriteLine();

            }

        }

        private int GetMaxCustomerID()
        {

            int maxId = Int32.MinValue;

            OdbcConnection connection = new OdbcConnection(_connString);

            try
            {
                connection.Open();
                // Console.WriteLine("Connection State: " + connection.State.ToString());
                // Console.WriteLine();

                OdbcDataReader reader = null;
                try
                {
                    string query = "SELECT MAX(customer_id) AS max_id FROM test.customer";
                    OdbcCommand command = new OdbcCommand(query, connection);

                    reader = command.ExecuteReader();

                    if (reader.HasRows)
                    {
                        reader.Read();
                        maxId = reader.GetInt32(0);
                    }

                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
                finally
                {
                    if (reader != null) {
                        reader.Close();
                    }
                }
                
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                connection.Close();
                // Console.WriteLine("Connection State: " + connection.State);
                // Console.WriteLine();

            }

            return maxId;
        }

    }
}