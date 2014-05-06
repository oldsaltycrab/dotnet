using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Common;
using System.Linq;
using System.Text;

using StackExchange.Profiling.Helpers;
using StackExchange.Profiling.Helpers.Dapper;

using Npgsql;

namespace StackExchange.Profiling.Storage
{
	public class PostgresServerStorage : DatabaseStorageBase
	{
/// <summary>
        /// Load the SQL statements (using Dapper Multiple Results)
        /// </summary>
        private const string SqlStatements = @"
SELECT * FROM MiniProfilers WHERE Id = @id;
SELECT * FROM MiniProfilerTimings WHERE MiniProfilerId = @id ORDER BY StartMilliseconds;
SELECT * FROM MiniProfilerClientTimings WHERE MiniProfilerId = @id ORDER BY Start;";

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlServerStorage"/> class with the specified connection string.
        /// </summary>
        /// <param name="connectionString">
        /// The connection string to use.
        /// </param>
        public PostgresServerStorage(string connectionString)
            : base(connectionString)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlServerStorage"/> class with the specified connection string settings.
        /// </summary>
        /// <param name="connectionStringSettings">
        /// The connection string settings read from ConfigurationManager.ConnectionStrings["connection"]
        /// </param>
        public PostgresServerStorage(ConnectionStringSettings connectionStringSettings)
            :base(connectionStringSettings.ConnectionString)
        {
        }
        
        /// <summary>
        /// Stores to <c>dbo.MiniProfilers</c> under its <see cref="MiniProfiler.Id"/>;
        /// </summary>
        /// <param name="profiler">The Mini Profiler</param>
        public override void Save(MiniProfiler profiler)
        {
            const string sql =
@"insert into MiniProfilers
            (Id,
             RootTimingId,
             Started,
             DurationMilliseconds,
             UserName,
             HasUserViewed,
             MachineName)
select       @Id,
             @RootTimingId,
             @Started,
             @DurationMilliseconds,
             @UserName,
             @HasUserViewed,
             @MachineName
where not exists (select 1 from MiniProfilers where Id = @Id)";

            using (var conn = GetOpenConnection())
            {
                conn.Execute(
                    sql,
                    new
                        {
                            profiler.Id,
                            profiler.Started,
                            UserName = profiler.UserName.Truncate(100),
                            RootTimingId = profiler.Root != null ? profiler.Root.Id : (Guid?)null,
                            profiler.DurationMilliseconds,
                            profiler.HasUserViewed,
                            MachineName = profiler.MachineName.Truncate(100)
                        });

                var timings = new List<Timing>();
                if (profiler.Root != null)
                {
                    profiler.Root.MiniProfilerId = profiler.Id;
                    FlattenTimings(profiler.Root, timings);
                }

                SaveTimings(timings, conn);
            }
        }

        private void SaveTimings(List<Timing> timings, DbConnection conn)
        {
            const string sql = @"INSERT INTO MiniProfilerTimings
            (Id,
             MiniProfilerId,
             ParentTimingId,
             Name,
             DurationMilliseconds,
             StartMilliseconds,
             IsRoot,
             Depth,
             CustomTimingsJson)
SELECT       @Id,
             @MiniProfilerId,
             @ParentTimingId,
             @Name,
             @DurationMilliseconds,
             @StartMilliseconds,
             @IsRoot,
             @Depth,
             @CustomTimingsJson
WHERE NOT EXISTS (SELECT 1 FROM MiniProfilerTimings WHERE Id = @Id)";

            foreach (var timing in timings)
            {
                conn.Execute(
                    sql, 
                    new
                        {
                            timing.Id,
                            timing.MiniProfilerId,
                            timing.ParentTimingId,
                            Name = timing.Name.Truncate(200),
                            timing.DurationMilliseconds,
                            timing.StartMilliseconds,
                            timing.IsRoot,
                            timing.Depth,
                            timing.CustomTimingsJson
                        });
            }
        }

        private void FlattenTimings(Timing timing, List<Timing> timingsCollection)
        {
            timingsCollection.Add(timing);
            if (timing.HasChildren)
            {
                timing.Children.ForEach(x => FlattenTimings(x, timingsCollection));
            }
        }

        /// <summary>
        /// Loads the <c>MiniProfiler</c> identified by 'id' from the database.
        /// </summary>
        /// <param name="id">The id.</param>
        /// <returns>the mini profiler.</returns>
        public override MiniProfiler Load(Guid id)
        {
            using (var conn = GetOpenConnection())
            {
                var idParameter = new { id };
                var result = LoadProfilerRecord(conn, idParameter);

                if (result != null)
                {
                    // HACK: stored dates are utc, but are pulled out as local time
                    result.Started = new DateTime(result.Started.Ticks, DateTimeKind.Utc);
                }

                return result;
            }
        }

        /// <summary>
        /// Sets the session to un-viewed 
        /// </summary>
        /// <param name="user">The user.</param>
        /// <param name="id">The id.</param>
        public override void SetUnviewed(string user, Guid id)
        {
            using (var conn = GetOpenConnection())
            {
                conn.Execute("UPDATE MiniProfilers SET HasUserViewed = 0 WHERE Id = @id AND UserName = @user", new { id, user });
            }
        }

        /// <summary>
        /// sets the session to viewed
        /// </summary>
        /// <param name="user">The user.</param>
        /// <param name="id">The id.</param>
        public override void SetViewed(string user, Guid id)
        {
            using (var conn = GetOpenConnection())
            {
                conn.Execute("UPDATE MiniProfilers SET HasUserViewed = 1 WHERE Id = @id AND UserName = @user", new { id, user });
            }
        }

        /// <summary>
        /// Returns a list of <see cref="MiniProfiler.Id"/>s that haven't been seen by <paramref name="user"/>.
        /// </summary>
        /// <param name="user">User identified by the current UserProvider"/&gt;.</param>
        /// <returns>the list of keys.</returns>
        public override List<Guid> GetUnviewedIds(string user)
        {
            const string Sql =
                @"select Id
                from   MiniProfilers
                where  UserName = @user
                and    HasUserViewed = 0
                order  by Started";

            using (var conn = GetOpenConnection())
            {
                return conn.Query<Guid>(Sql, new { user }).ToList();
            }
        }

        /// <summary>
        /// List the Results.
        /// </summary>
        /// <param name="maxResults">The max results.</param>
        /// <param name="start">The start.</param>
        /// <param name="finish">The finish.</param>
        /// <param name="orderBy">order by.</param>
        /// <returns>the list of key values.</returns>
        public override IEnumerable<Guid> List(int maxResults, DateTime? start = null, DateTime? finish = null, ListResultsOrder orderBy = ListResultsOrder.Descending)
        {
            var builder = new SqlBuilder();
            var t = builder.AddTemplate("select top " + maxResults + " Id from MiniProfilers /**where**/ /**orderby**/");

            if (finish != null)
            {
                builder.Where("Started < @finish", new { finish });
            }

            if (start != null)
            {
                builder.Where("Started > @start", new { start });
            }

            builder.OrderBy(orderBy == ListResultsOrder.Descending ? "Started desc" : "Started asc");

            using (var conn = GetOpenConnection())
            {
                return conn.Query<Guid>(t.RawSql, t.Parameters).ToList();
            }
        }
        
        /// <summary>
        /// Load individual MiniProfiler
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="keyParameter">The id Parameter.</param>
        /// <returns>Related MiniProfiler object</returns>
        private MiniProfiler LoadProfilerRecord(DbConnection connection, object keyParameter)
        {
            MiniProfiler profiler;
            using (var multi = connection.QueryMultiple(SqlStatements, keyParameter))
            {
                profiler = multi.Read<MiniProfiler>().SingleOrDefault();
                var timings = multi.Read<Timing>().ToList();
                var clientTimings = multi.Read<ClientTimings.ClientTiming>().ToList();

                if (profiler != null && profiler.RootTimingId.HasValue && timings.Any())
                {
                    var rootTiming = timings.SingleOrDefault(x => x.Id == profiler.RootTimingId.Value);
                    if (rootTiming != null)
                    {
                        profiler.Root = rootTiming;
                        timings.ForEach(x => x.Profiler = profiler);
                        timings.Remove(rootTiming);
                        var timingsLookupByParent = timings.ToLookup(x => x.ParentTimingId, x => x);
                        PopulateChildTimings(rootTiming, timingsLookupByParent);
                    }
                    if (clientTimings.Any() || profiler.ClientTimingsRedirectCount.HasValue)
                    {
                        profiler.ClientTimings = new ClientTimings
                        {
                            RedirectCount = profiler.ClientTimingsRedirectCount ?? 0, 
                            Timings = clientTimings
                        };
                    }
                }
            }
            return profiler;
        }

        /// <summary>
        /// Build the subtree of <see cref="Timing"/> objects with <paramref name="parent"/> at the top.
        /// Used recursively.
        /// </summary>
        /// <param name="parent">Parent <see cref="Timing"/> to be evaluated.</param>
        /// <param name="timingsLookupByParent">Key: parent timing Id; Value: collection of all <see cref="Timing"/> objects under the given parent.</param>
        private void PopulateChildTimings(Timing parent, ILookup<Guid, Timing> timingsLookupByParent)
        {
            if (timingsLookupByParent.Contains(parent.Id))
            {
                foreach (var timing in timingsLookupByParent[parent.Id].OrderBy(x => x.StartMilliseconds))
                {
                    parent.AddChild(timing);
                    PopulateChildTimings(timing, timingsLookupByParent);
                }
            }
        }

        /// <summary>
        /// Returns a connection to Postgres Server.
        /// </summary>
        protected virtual DbConnection GetConnection()
        {
            return new NpgsqlConnection(ConnectionString);
        }

        /// <summary>
        /// Returns a DbConnection already opened for execution.
        /// </summary>
        protected DbConnection GetOpenConnection()
        {
            var result = GetConnection();
            if (result.State != System.Data.ConnectionState.Open)
                result.Open();
            return result;
        }

        /// <summary>
        /// Creates needed tables. Run this once on your database.
        /// </summary>
        /// <remarks>
        /// Works in Postgres.
        /// </remarks>
        public static readonly string TableCreationScript =
                @"
                CREATE TABLE MiniProfilers
                  (
                     RowId                                BIGSERIAL NOT NULL,
                     Id                                   UUID NOT NULL,
                     RootTimingId                         UUID NULL,
                     Started                              TIMESTAMP NOT NULL,
                     DurationMilliseconds                 REAL NOT NULL,
                     UserName                             TEXT NULL,
                     HasUserViewed                        BOOLEAN NOT NULL,
                     MachineName                          TEXT NULL,
					 CONSTRAINT PK_MiniProfilers PRIMARY KEY (RowId)
                  );
                
                -- displaying results selects everything based on the main MiniProfilers.Id column
                CREATE UNIQUE INDEX IX_MiniProfilers_Id on MiniProfilers (Id);

                CREATE TABLE MiniProfilerTimings
                  (
                     RowId                               BIGSERIAL NOT NULL,
                     Id                                  UUID NOT NULL,
                     MiniProfilerId                      UUID NOT NULL,
                     ParentTimingId                      UUID NULL,
                     Name                                TEXT NOT NULL,
                     DurationMilliseconds                REAL NOT NULL,
                     StartMilliseconds                   REAL NOT NULL,
                     IsRoot                              BOOLEAN NOT NULL,
                     Depth                               SMALLINT NOT NULL,
                     CustomTimingsJson                   TEXT NULL,
					 CONSTRAINT PK_MiniProfilerTimings PRIMARY KEY (RowId)
                  );

				 CREATE UNIQUE INDEX IX_MiniProfilerTimings_Id ON IX_MiniProfilerTimings_Id (Id);
                 CREATE INDEX IX_MiniProfilerTimings_MiniProfilerId ON MiniProfilerTimings (MiniProfilerId);        
                ";
	}
}
