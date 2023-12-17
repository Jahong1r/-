using Dapper;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using MPI;
using Microsoft.Identity.Client;

namespace ConsoleApp4
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Repository repository = new Repository();
                Insert(repository, args);
        }

        static void Insert(Repository userRepository, string[] args) { 
            MPI.Environment.Run(ref args, comm =>
                {
                    var myRank = MPI.Communicator.world.Rank;
                    var size = MPI.Communicator.world.Size;
                    int arraySize = 4000;
                    if (!Int32.TryParse(args[0], out arraySize))
                    {
                        arraySize = 1000;
                    }
                    var array = new List<string>(arraySize);
                    DateTime start = DateTime.Now;
                    if (myRank == 0)
                    {
                        for (var i = 0; i < arraySize; i++)
                        {
                            array.Add(" SELECT 'Продукт OLOLO', '1', '1', '19', '1' UNION ALL");
                        }
                        if (size > 1)
                        {
                            for (int i = 1; i < size; i++)
                            {
                                Communicator.world.Send(array, i, myRank);
                            }
                        }
                    }

                    if (myRank > 0)
                    {
                        array = Communicator.world.Receive<List<string>>(0, 0);
                    }
                    var query = "INSERT INTO Production.Products(productname, supplierid, categoryid, unitprice, discontinued)";

                    for (var i = myRank * (arraySize / size); i < (myRank + 1) * (arraySize / size); i++)
                    {
                        query = query + array[i];
                    }
                    userRepository.Create(query.Substring(0, query.Length - 9));
                    Communicator.world.Barrier();
                    if (myRank == 0)
                    {
                        DateTime end = DateTime.Now;
                        TimeSpan ts = (end - start);
                        Console.WriteLine("Заняло {0} мс", ts.TotalMilliseconds);
                    }
                });
            }
    }

 /*   [Serializable]
    public class Prod
    {
        public string productname { get; set; }
        public int supplierid { get; set; }
        public int categoryid { get; set; }
        public int unitprice { get; set; }
        public int discontinued { get; set; }

        public Prod(string productname, int supplierid, int categoryid, int unitprice, int discontinued)
        {
            this.productname = productname;
            this.supplierid = supplierid;
            this.categoryid = categoryid;
            this.unitprice = unitprice;
            this.discontinued = discontinued;
        }
    }
 */

    public class Repository 
    {
        string connectionString = "Server=.\\SQLEXPRESS;Initial Catalog=TSQL2012; Encrypt=false; TrustServerCertificate=true; Integrated Security=True";
        public Repository()
        {
            
        }

        /*public List<Prod> GetContact(string query)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                return db.Query<Prod>(query).ToList();
            }
        }*/

        public void Create(string query)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                db.Execute(query);
            }
        }
    }
}