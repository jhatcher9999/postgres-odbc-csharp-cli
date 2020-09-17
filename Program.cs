using System;

namespace postgres_odbc_csharp_cli
{
    class Program
    {
        static void Main(string[] args)
        {

            OdbcTester odbcTester = new OdbcTester();
            odbcTester.Run();

        }
    }
}
