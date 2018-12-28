using System.Data;
using System.Threading.Tasks;

namespace Miningcore.Persistence.Repositories
{
    public interface IProjectsRepository
    {
        Task<bool> ProjectExists(IDbConnection con, long projectID);
    }
}