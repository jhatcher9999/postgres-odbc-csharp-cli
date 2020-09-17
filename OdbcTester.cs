using System;
using System.Data.Odbc;
using System.Text;
using System.Collections;
using System.Collections.Generic;

namespace postgres_odbc_csharp_cli
{
    class OdbcTester
    {

        private string _connString = "DSN=CockroachDB;Trusted_Connection=Yes";

        private Random random = new Random((int)DateTime.Now.Ticks);


        public void Run()
        {

            int testNumber = 1;

            Console.WriteLine("Starting TEST " + testNumber++ + ": Connection Options");
            Console.WriteLine("=============================================");
            Console.WriteLine();
            ConnectionOptions();

            Console.WriteLine("Starting TEST " + testNumber++ + ": Querying Data");
            Console.WriteLine("=============================================");
            Console.WriteLine();
            ReadData();

            int maxId = GetMaxCustomerID();

            Console.WriteLine("Starting TEST " + testNumber++ + ": Inserting Data");
            Console.WriteLine("=============================================");
            Console.WriteLine();
            WriteData(maxId + 1);
        }

        private void ConnectionOptions()
        {

            Dictionary<string, string> connStrings = new Dictionary<string, string> {
                { "DSN=CockroachDB",                                                                              "DSN with no authentication options given" },
                { "DSN=CockroachDB;Trusted_Connection=Yes",                                                       "DSN with trusted connection" },
                { "DSN=CockroachDB;Uid=;Pwd=",                                                                    "DSN with blank username/password" },
                { "DSN=CockroachDB;Uid=jimhatcher;Pwd=",                                                          "DSN with username specified but a blank password" },
                { "Driver={PostgreSQL Driver};Server=localhost;Port=26257;Database=test;Trusted_Connection=Yes;", "DSN-less with trusted connection" },
                { "Driver={PostgreSQL Driver};Server=localhost;Port=26257;Database=test;Uid=;Pwd=;",              "DSN-less with blank username/password" },
            };

            foreach(KeyValuePair<string, string> kvp in connStrings)
            {
                string connStringValue = kvp.Key;
                string connStringDescription = kvp.Value;

                Console.WriteLine("Attempting connection ...");
                Console.WriteLine("Connection String: " + connStringValue);
                Console.WriteLine("Scenario: " + connStringDescription);
                OdbcConnection connection = new OdbcConnection(connStringValue);
                try
                {

                    connection.Open();

                    Console.WriteLine("Success");
                }
                catch(Exception ex)
                {
                    Console.WriteLine("Failure");
                    Console.WriteLine(ex.Message);
                }
                finally
                {
                    connection.Close();
                    // Console.WriteLine("Connection State: " + connection.State);
                    // Console.WriteLine();

                }
                Console.WriteLine();
            }

        }

        private void ReadData()
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
                    string query = "SELECT * FROM test.customer ORDER BY customer_id DESC LIMIT 5;";
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

                    Console.WriteLine("Read successful");
                    Console.WriteLine();
                    
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

        private void WriteData(int customerID)
        {
            string name = GetRandomString(5);
            WriteData(customerID, name);
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

        private void WriteData(int customerID, string name)
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

                    Console.WriteLine("INSERT successful");
                    Console.WriteLine();

                    string query2 = "UPDATE test.customer SET name = ? WHERE customer_id = ?;";
                    string newName = GetRandomString(5);
                    OdbcCommand command2 = new OdbcCommand(query2, connection);

                    command2.Parameters.Add("@name", OdbcType.VarChar).Value = newName;
                    command2.Parameters.Add("@customer_id", OdbcType.Int).Value = customerID;

                    Console.WriteLine("Running UPDATE query with values " + customerID.ToString() + ", " + newName);
                    command2.ExecuteNonQuery();

                    Console.WriteLine("UPDATE successful");
                    Console.WriteLine();

                    string query3 = "DELETE FROM test.customer WHERE customer_id = ?;";
                    OdbcCommand command3 = new OdbcCommand(query3, connection);

                    command3.Parameters.Add("@customer_id", OdbcType.Int).Value = customerID;

                    Console.WriteLine("Running DELETE query with values " + customerID.ToString());
                    command3.ExecuteNonQuery();

                    Console.WriteLine("DELETE successful");
                    Console.WriteLine();

                    //put the data back so we will keep some values around for our read queries
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