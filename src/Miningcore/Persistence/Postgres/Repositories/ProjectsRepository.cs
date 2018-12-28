using System.Data;
using System.Threading.Tasks;
using Dapper;
using Miningcore.Extensions;
using Miningcore.Persistence.Repositories;
using NLog;

namespace Miningcore.Persistence.Postgres.Repositories
{
    public class ProjectsRepository : IProjectsRepository
    {
        private static readonly ILogger logger = LogManager.GetCurrentClassLogger();
        
        public async Task<bool> ProjectExists(IDbConnection con, long projectID)
        {
            logger.LogInvoke();
            
            const string query = "SELECT EXISTS(SELECT 1 FROM projects WHERE id = @projectid)";

            return await con.QuerySingleOrDefaultAsync<bool>(query, new { projectID });
        }
    }
}