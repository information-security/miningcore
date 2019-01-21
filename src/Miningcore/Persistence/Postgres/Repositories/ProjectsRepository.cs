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
        
        public async Task<long> GetProjectId(IDbConnection con, string userAddress)
        {
            logger.LogInvoke();

            const string query =
                "SELECT ump.project_id FROM user_mining_projects AS ump LEFT JOIN users AS u ON ump.user_id = u.id WHERE u.eth_address = @userAddress";

            return await con.QuerySingleOrDefaultAsync<long>(query, new { userAddress });
        }
    }
}