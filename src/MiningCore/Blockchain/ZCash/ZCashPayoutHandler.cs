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
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using AutoMapper;
using MiningCore.Blockchain.Bitcoin;
using MiningCore.Blockchain.Bitcoin.DaemonResponses;
using MiningCore.Blockchain.ZCash.Configuration;
using MiningCore.Blockchain.ZCash.DaemonRequests;
using MiningCore.Blockchain.ZCash.DaemonResponses;
using MiningCore.Configuration;
using MiningCore.Extensions;
using MiningCore.Messaging;
using MiningCore.Persistence;
using MiningCore.Persistence.Model;
using MiningCore.Persistence.Repositories;
using MiningCore.Time;
using Newtonsoft.Json.Linq;
using Contract = MiningCore.Contracts.Contract;

namespace MiningCore.Blockchain.ZCash
{
    [CoinMetadata(CoinType.ZEC, CoinType.ZCL, CoinType.ZEN, CoinType.BTCP)]
    public class ZCashPayoutHandler : BitcoinPayoutHandler
    {
        public ZCashPayoutHandler(
            IComponentContext ctx,
            IConnectionFactory cf,
            IMapper mapper,
            IShareRepository shareRepo,
            IBlockRepository blockRepo,
            IBalanceRepository balanceRepo,
            IPaymentRepository paymentRepo,
            IMasterClock clock,
            IMessageBus messageBus) :
            base(ctx, cf, mapper, shareRepo, blockRepo, balanceRepo, paymentRepo, clock, messageBus)
        {
        }

        protected ZCashPoolConfigExtra poolExtraConfig;
        protected bool supportsNativeShielding;
        protected BitcoinNetworkType networkType;
        protected ZCashChainConfig chainConfig;
        protected override string LogCategory => "ZCash Payout Handler";
        protected const decimal TransferFee = 0.0001m;
        protected const int ZMinConfirmations = 8;

        #region IPayoutHandler

        public override async Task ConfigureAsync(ClusterConfig clusterConfig, PoolConfig poolConfig)
        {
            await base.ConfigureAsync(clusterConfig, poolConfig);

            poolExtraConfig = poolConfig.Extra.SafeExtensionDataAs<ZCashPoolConfigExtra>();

            // detect network
            var blockchainInfoResponse = await daemon.ExecuteCmdSingleAsync<BlockchainInfo>(BitcoinCommands.GetBlockchainInfo);

            if (blockchainInfoResponse.Response.Chain.ToLower() == "test")
                networkType = BitcoinNetworkType.Test;
            else if (blockchainInfoResponse.Response.Chain.ToLower() == "regtest")
                networkType = BitcoinNetworkType.RegTest;
            else
                networkType = BitcoinNetworkType.Main;

            // lookup config
            if (ZCashConstants.Chains.TryGetValue(poolConfig.Coin.Type, out var coinbaseTx))
                coinbaseTx.TryGetValue(networkType, out chainConfig);

            // detect z_shieldcoinbase support
            var response = await daemon.ExecuteCmdSingleAsync<JObject>(ZCashCommands.ZShieldCoinbase);
            supportsNativeShielding = response.Error.Code != (int) BitcoinRPCErrorCode.RPC_METHOD_NOT_FOUND;
        }

        #endregion // IPayoutHandler
    }
}
