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
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using AutoMapper;
using MiningCore.Blockchain.Monero.Configuration;
using MiningCore.Blockchain.Monero.DaemonRequests;
using MiningCore.Blockchain.Monero.DaemonResponses;
using MiningCore.Configuration;
using MiningCore.DaemonInterface;
using MiningCore.Extensions;
using MiningCore.Messaging;
using MiningCore.Native;
using MiningCore.Notifications;
using MiningCore.Payments;
using MiningCore.Persistence;
using MiningCore.Persistence.Model;
using MiningCore.Persistence.Repositories;
using MiningCore.Time;
using MiningCore.Util;
using Newtonsoft.Json;
using Contract = MiningCore.Contracts.Contract;
using MC = MiningCore.Blockchain.Monero.MoneroCommands;
using MWC = MiningCore.Blockchain.Monero.MoneroWalletCommands;

namespace MiningCore.Blockchain.Monero
{
    [CoinMetadata(CoinType.XMR, CoinType.AEON, CoinType.ETN)]
    public class MoneroPayoutHandler : PayoutHandlerBase,
        IPayoutHandler
    {
        public MoneroPayoutHandler(
            IComponentContext ctx,
            IConnectionFactory cf,
            IMapper mapper,
            IShareRepository shareRepo,
            IBlockRepository blockRepo,
            IBalanceRepository balanceRepo,
            IPaymentRepository paymentRepo,
            IMasterClock clock,
            IMessageBus messageBus) :
            base(cf, mapper, shareRepo, blockRepo, balanceRepo, paymentRepo, clock, messageBus)
        {
            Contract.RequiresNonNull(ctx, nameof(ctx));
            Contract.RequiresNonNull(balanceRepo, nameof(balanceRepo));
            Contract.RequiresNonNull(paymentRepo, nameof(paymentRepo));

            this.ctx = ctx;
        }

        private readonly IComponentContext ctx;
        private DaemonClient daemon;
        private DaemonClient walletDaemon;
        private MoneroNetworkType? networkType;
        private MoneroPoolPaymentProcessingConfigExtra extraConfig;
        private bool walletSupportsTransferSplit;

        protected override string LogCategory => "Monero Payout Handler";

        private async Task<MoneroNetworkType> GetNetworkTypeAsync()
        {
            if (!networkType.HasValue)
            {
                var infoResponse = await daemon.ExecuteCmdAnyAsync(MC.GetInfo, true);
                var info = infoResponse.Response.ToObject<GetInfoResponse>();

                networkType = info.IsTestnet ? MoneroNetworkType.Test : MoneroNetworkType.Main;
            }

            return networkType.Value;
        }

        private void ExtractAddressAndPaymentId(string input, out string address, out string paymentId)
        {
            paymentId = null;
            var index = input.IndexOf(PayoutConstants.PayoutInfoSeperator);

            if (index != -1)
            {
                address = input.Substring(0, index);

                if (index + 1 < input.Length)
                {
                    paymentId = input.Substring(index + 1);

                    // ignore invalid payment ids
                    if (paymentId.Length != MoneroConstants.PaymentIdHexLength)
                        paymentId = null;
                }
            }

            else
                address = input;
        }

        #region IPayoutHandler

        public async Task ConfigureAsync(ClusterConfig clusterConfig, PoolConfig poolConfig)
        {
            Contract.RequiresNonNull(poolConfig, nameof(poolConfig));

            this.poolConfig = poolConfig;
            this.clusterConfig = clusterConfig;
            extraConfig = poolConfig.PaymentProcessing.Extra.SafeExtensionDataAs<MoneroPoolPaymentProcessingConfigExtra>();

            logger = LogUtil.GetPoolScopedLogger(typeof(MoneroPayoutHandler), poolConfig);

            // configure standard daemon
            var jsonSerializerSettings = ctx.Resolve<JsonSerializerSettings>();

            var daemonEndpoints = poolConfig.Daemons
                .Where(x => string.IsNullOrEmpty(x.Category))
                .Select(x =>
                {
                    if (string.IsNullOrEmpty(x.HttpPath))
                        x.HttpPath = MoneroConstants.DaemonRpcLocation;

                    return x;
                })
                .ToArray();

            daemon = new DaemonClient(jsonSerializerSettings, messageBus, clusterConfig.ClusterName ?? poolConfig.PoolName, poolConfig.Id);
            daemon.Configure(daemonEndpoints);

            // configure wallet daemon
            var walletDaemonEndpoints = poolConfig.Daemons
                .Where(x => x.Category?.ToLower() == MoneroConstants.WalletDaemonCategory)
                .Select(x =>
                {
                    if (string.IsNullOrEmpty(x.HttpPath))
                        x.HttpPath = MoneroConstants.DaemonRpcLocation;

                    return x;
                })
                .ToArray();

            walletDaemon = new DaemonClient(jsonSerializerSettings, messageBus, clusterConfig.ClusterName ?? poolConfig.PoolName, poolConfig.Id);
            walletDaemon.Configure(walletDaemonEndpoints);

            // detect network
            await GetNetworkTypeAsync();

            // detect transfer_split support
            var response = await walletDaemon.ExecuteCmdSingleAsync<TransferResponse>(MWC.TransferSplit);
            walletSupportsTransferSplit = response.Error.Code != MoneroConstants.MoneroRpcMethodNotFound;
        }

        public async Task<Block[]> ClassifyBlocksAsync(Block[] blocks)
        {
            Contract.RequiresNonNull(poolConfig, nameof(poolConfig));
            Contract.RequiresNonNull(blocks, nameof(blocks));

            var pageSize = 100;
            var pageCount = (int) Math.Ceiling(blocks.Length / (double) pageSize);
            var result = new List<Block>();

            for(var i = 0; i < pageCount; i++)
            {
                // get a page full of blocks
                var page = blocks
                    .Skip(i * pageSize)
                    .Take(pageSize)
                    .ToArray();

                // NOTE: monerod does not support batch-requests
                for(var j = 0; j < page.Length; j++)
                {
                    var block = page[j];

                    var rpcResult = await daemon.ExecuteCmdAnyAsync<GetBlockHeaderResponse>(
                        MC.GetBlockHeaderByHeight,
                        new GetBlockHeaderByHeightRequest
                        {
                            Height = block.BlockHeight
                        });

                    if (rpcResult.Error != null)
                    {
                        logger.Debug(() => $"[{LogCategory}] Daemon reports error '{rpcResult.Error.Message}' (Code {rpcResult.Error.Code}) for block {block.BlockHeight}");
                        continue;
                    }

                    if (rpcResult.Response?.BlockHeader == null)
                    {
                        logger.Debug(() => $"[{LogCategory}] Daemon returned no header for block {block.BlockHeight}");
                        continue;
                    }

                    var blockHeader = rpcResult.Response.BlockHeader;

                    // update progress
                    block.ConfirmationProgress = Math.Min(1.0d, (double) blockHeader.Depth / MoneroConstants.PayoutMinBlockConfirmations);
                    result.Add(block);

                    // orphaned?
                    if (blockHeader.IsOrphaned || blockHeader.Hash != block.TransactionConfirmationData)
                    {
                        block.Status = BlockStatus.Orphaned;
                        block.Reward = 0;
                        continue;
                    }

                    // matured and spendable?
                    if (blockHeader.Depth >= MoneroConstants.PayoutMinBlockConfirmations)
                    {
                        block.Status = BlockStatus.Confirmed;
                        block.ConfirmationProgress = 1;
                        block.Reward = (decimal) blockHeader.Reward / MoneroConstants.SmallestUnit[poolConfig.Coin.Type];

                        logger.Info(() => $"[{LogCategory}] Unlocked block {block.BlockHeight} worth {FormatAmount(block.Reward)}");
                    }
                }
            }

            return result.ToArray();
        }

        public Task CalculateBlockEffortAsync(Block block, double accumulatedBlockShareDiff)
        {
            block.Effort = accumulatedBlockShareDiff / block.NetworkDifficulty;

            return Task.FromResult(true);
        }

        public Task<decimal> UpdateBlockRewardBalancesAsync(IDbConnection con, IDbTransaction tx, Block block, PoolConfig pool)
        {
            var blockRewardRemaining = block.Reward;

            // Distribute funds to configured reward recipients
            foreach(var recipient in poolConfig.RewardRecipients.Where(x => x.Percentage > 0))
            {
                var amount = block.Reward * (recipient.Percentage / 100.0m);
                var address = recipient.Address;

                blockRewardRemaining -= amount;

                // skip transfers from pool wallet to pool wallet
                if (address != poolConfig.Address)
                {
                    logger.Info(() => $"Adding {FormatAmount(amount)} to balance of {address}");
                    balanceRepo.AddAmount(con, tx, block.ProjectId, poolConfig.Id, poolConfig.Coin.Type, address, amount, $"Reward for block {block.BlockHeight}");
                }
            }

            // Deduct static reserve for tx fees
            blockRewardRemaining -= MoneroConstants.StaticTransactionFeeReserve;

            return Task.FromResult(blockRewardRemaining);
        }

        #endregion // IPayoutHandler
    }
}
