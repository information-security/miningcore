using System.Data;
using System.Threading.Tasks;

namespace Miningcore.Persistence.Repositories
{
    public interface IProjectsRepository
    {
        Task<long> GetProjectId(IDbConnection con, string userAddress);
    }
}