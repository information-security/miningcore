/*
Copyright 2017 Coin Foundry (coinfoundry.org)
Authors: Oliver Weichhold (oliver@weichhold.com)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System;
using System.Data;
using System.Linq;
using AutoMapper;
using Dapper;
using MiningCore.Extensions;
using MiningCore.Persistence.Model;
using MiningCore.Persistence.Repositories;
using MiningCore.Util;
using NLog;

namespace MiningCore.Persistence.Postgres.Repositories
{
    public class BlockRepository : IBlockRepository
    {
        public BlockRepository(IMapper mapper)
        {
            this.mapper = mapper;
        }

        private readonly IMapper mapper;
        private static readonly ILogger logger = LogManager.GetCurrentClassLogger();

        public void Insert(IDbConnection con, IDbTransaction tx, Block block)
        {
            logger.LogInvoke();

            var mapped = mapper.Map<Entities.Block>(block);

            var query =
                "INSERT INTO blocks(projectid, poolid, blockheight, networkdifficulty, status, type, transactionconfirmationdata, miner, reward, effort, confirmationprogress, source, hash, created) " +
                "VALUES(@projectid, @poolid, @blockheight, @networkdifficulty, @status, @type, @transactionconfirmationdata, @miner, @reward, @effort, @confirmationprogress, @source, @hash, @created)";

            con.Execute(query, mapped, tx);
        }

        public void DeleteBlock(IDbConnection con, IDbTransaction tx, Block block)
        {
            logger.LogInvoke();

            var query = "DELETE FROM blocks WHERE id = @id";
            con.Execute(query, block, tx);
        }

        public void UpdateBlock(IDbConnection con, IDbTransaction tx, Block block)
        {
            logger.LogInvoke();

            var mapped = mapper.Map<Entities.Block>(block);

            var query = "UPDATE blocks SET blockheight = @blockheight, status = @status, type = @type, reward = @reward, effort = @effort, confirmationprogress = @confirmationprogress WHERE id = @id";
            con.Execute(query, mapped, tx);
        }

        public Block[] PageBlocks(IDbConnection con, string projectId, string poolId, BlockStatus[] status, int page, int pageSize)
        {
            logger.LogInvoke(new[] { poolId });

            var query = "SELECT * FROM blocks WHERE projectid = @projectid AND poolid = @poolid AND status = ANY(@status) " +
                "ORDER BY created DESC OFFSET @offset FETCH NEXT (@pageSize) ROWS ONLY";

            return con.Query<Entities.Block>(query, new
                {
                    projectId,    
                    poolId,
                    status = status.Select(x => x.ToString().ToLower()).ToArray(),
                    offset = page * pageSize,
                    pageSize
                })
                .Select(mapper.Map<Block>)
                .ToArray();
        }

        public Block[] GetPendingBlocksForPool(IDbConnection con, string projectId, string poolId)
        {
            logger.LogInvoke(new[] { poolId });

            var query = "SELECT * FROM blocks WHERE projectid = @projectid AND poolid = @poolid AND status = @status";

            return con.Query<Entities.Block>(query, new { status = BlockStatus.Pending.ToString().ToLower(),
                    poolid = poolId, projectid = projectId  })
                .Select(mapper.Map<Block>)
                .ToArray();
        }

        public Block GetBlockBefore(IDbConnection con, string projectId, string poolId, BlockStatus[] status, DateTime before)
        {
            logger.LogInvoke(new[] { poolId });

            var query = "SELECT * FROM blocks WHERE projectid = @projectid AND poolid = @poolid AND status = ANY(@status) AND created < @before " +
                "ORDER BY created DESC FETCH NEXT (1) ROWS ONLY";

            return con.Query<Entities.Block>(query, new
                {
                    projectId,
                    poolId,
                    before,
                    status = status.Select(x => x.ToString().ToLower()).ToArray()
                })
                .Select(mapper.Map<Block>)
                .FirstOrDefault();
        }
    }
}
