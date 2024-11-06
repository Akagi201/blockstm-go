package blockstm

import (
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/beacon"
	beaconConsensus "github.com/ethereum/go-ethereum/consensus/beacon"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

var (
	// testKey is a private key to use for funding a tester account.
	testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

	// testAddr is the Ethereum address of the tester account.
	testAddr = crypto.PubkeyToAddress(testKey.PublicKey)

	testBalance = big.NewInt(2e18)
)

func TestSerialBlockExecutor(t *testing.T) {
	var (
		config = &params.ChainConfig{
			ChainID:                       big.NewInt(1),
			HomesteadBlock:                big.NewInt(0),
			EIP150Block:                   big.NewInt(0),
			EIP155Block:                   big.NewInt(0),
			EIP158Block:                   big.NewInt(0),
			ByzantiumBlock:                big.NewInt(0),
			ConstantinopleBlock:           big.NewInt(0),
			PetersburgBlock:               big.NewInt(0),
			IstanbulBlock:                 big.NewInt(0),
			MuirGlacierBlock:              big.NewInt(0),
			BerlinBlock:                   big.NewInt(0),
			LondonBlock:                   big.NewInt(0),
			Ethash:                        new(params.EthashConfig),
			TerminalTotalDifficulty:       big.NewInt(0),
			TerminalTotalDifficultyPassed: true,
			ShanghaiTime:                  new(uint64),
			CancunTime:                    new(uint64),
		}
		signer  = types.LatestSigner(config)
		key1, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key2, _ = crypto.HexToECDSA("0202020202020202020202020202020202020202020202020202002020202020")
	)
	var makeTx = func(key *ecdsa.PrivateKey, nonce uint64, to common.Address, amount *big.Int, gasLimit uint64, gasPrice *big.Int, data []byte) *types.Transaction {
		tx, _ := types.SignTx(types.NewTransaction(nonce, to, amount, gasLimit, gasPrice, data), signer, key)
		return tx
	}
	var mkDynamicTx = func(nonce uint64, to common.Address, gasLimit uint64, gasTipCap, gasFeeCap *big.Int) *types.Transaction {
		tx, _ := types.SignTx(types.NewTx(&types.DynamicFeeTx{
			Nonce:     nonce,
			GasTipCap: gasTipCap,
			GasFeeCap: gasFeeCap,
			Gas:       gasLimit,
			To:        &to,
			Value:     big.NewInt(0),
		}), signer, key1)
		return tx
	}
	var mkDynamicCreationTx = func(nonce uint64, gasLimit uint64, gasTipCap, gasFeeCap *big.Int, data []byte) *types.Transaction {
		tx, _ := types.SignTx(types.NewTx(&types.DynamicFeeTx{
			Nonce:     nonce,
			GasTipCap: gasTipCap,
			GasFeeCap: gasFeeCap,
			Gas:       gasLimit,
			Value:     big.NewInt(0),
			Data:      data,
		}), signer, key1)
		return tx
	}
	var mkBlobTx = func(nonce uint64, to common.Address, gasLimit uint64, gasTipCap, gasFeeCap, blobGasFeeCap *big.Int, hashes []common.Hash) *types.Transaction {
		tx, err := types.SignTx(types.NewTx(&types.BlobTx{
			Nonce:      nonce,
			GasTipCap:  uint256.MustFromBig(gasTipCap),
			GasFeeCap:  uint256.MustFromBig(gasFeeCap),
			Gas:        gasLimit,
			To:         to,
			BlobHashes: hashes,
			BlobFeeCap: uint256.MustFromBig(blobGasFeeCap),
			Value:      new(uint256.Int),
		}), signer, key1)
		if err != nil {
			t.Fatal(err)
		}
		return tx
	}
	hc, err := core.NewHeaderChain(rawdb.NewMemoryDatabase(), config, beacon.New(ethash.NewFaker()), nil)
	if err != nil {
		t.Fatal(err)
	}

	_ = key2
	_ = hc
	_ = makeTx
	_ = mkDynamicTx
	_ = mkDynamicCreationTx
	_ = mkBlobTx
}

// func generateBlock(parent *types.Block, engine consensus.Engine, txs types.Transactions, config *params.ChainConfig) *types.Block {
// 	return types.NewBlock(header, body, receipts, trie.NewStackTrie(nil))
// }

func generateMergeChain(n int, merged bool) (*core.Genesis, []*types.Block) {
	config := *params.AllEthashProtocolChanges
	engine := consensus.Engine(beaconConsensus.New(ethash.NewFaker()))
	if merged {
		config.TerminalTotalDifficulty = common.Big0
		config.TerminalTotalDifficultyPassed = true
		engine = beaconConsensus.NewFaker()
	}
	genesis := &core.Genesis{
		Config: &config,
		Alloc: types.GenesisAlloc{
			testAddr:                  {Balance: testBalance},
			params.BeaconRootsAddress: {Balance: common.Big0, Code: common.Hex2Bytes("3373fffffffffffffffffffffffffffffffffffffffe14604457602036146024575f5ffd5b620180005f350680545f35146037575f5ffd5b6201800001545f5260205ff35b6201800042064281555f359062018000015500")},
			// config.DepositContractAddress: {
			// 	// Simple deposit generator, source: https://gist.github.com/lightclient/54abb2af2465d6969fa6d1920b9ad9d7
			// 	Code:    common.Hex2Bytes("6080604052366103aa575f603067ffffffffffffffff811115610025576100246103ae565b5b6040519080825280601f01601f1916602001820160405280156100575781602001600182028036833780820191505090505b5090505f8054906101000a900460ff1660f81b815f8151811061007d5761007c6103db565b5b60200101907effffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff191690815f1a9053505f602067ffffffffffffffff8111156100c7576100c66103ae565b5b6040519080825280601f01601f1916602001820160405280156100f95781602001600182028036833780820191505090505b5090505f8054906101000a900460ff1660f81b815f8151811061011f5761011e6103db565b5b60200101907effffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff191690815f1a9053505f600867ffffffffffffffff811115610169576101686103ae565b5b6040519080825280601f01601f19166020018201604052801561019b5781602001600182028036833780820191505090505b5090505f8054906101000a900460ff1660f81b815f815181106101c1576101c06103db565b5b60200101907effffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff191690815f1a9053505f606067ffffffffffffffff81111561020b5761020a6103ae565b5b6040519080825280601f01601f19166020018201604052801561023d5781602001600182028036833780820191505090505b5090505f8054906101000a900460ff1660f81b815f81518110610263576102626103db565b5b60200101907effffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff191690815f1a9053505f600867ffffffffffffffff8111156102ad576102ac6103ae565b5b6040519080825280601f01601f1916602001820160405280156102df5781602001600182028036833780820191505090505b5090505f8054906101000a900460ff1660f81b815f81518110610305576103046103db565b5b60200101907effffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff191690815f1a9053505f8081819054906101000a900460ff168092919061035090610441565b91906101000a81548160ff021916908360ff160217905550507f649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c585858585856040516103a09594939291906104d9565b60405180910390a1005b5f80fd5b7f4e487b71000000000000000000000000000000000000000000000000000000005f52604160045260245ffd5b7f4e487b71000000000000000000000000000000000000000000000000000000005f52603260045260245ffd5b7f4e487b71000000000000000000000000000000000000000000000000000000005f52601160045260245ffd5b5f60ff82169050919050565b5f61044b82610435565b915060ff820361045e5761045d610408565b5b600182019050919050565b5f81519050919050565b5f82825260208201905092915050565b8281835e5f83830152505050565b5f601f19601f8301169050919050565b5f6104ab82610469565b6104b58185610473565b93506104c5818560208601610483565b6104ce81610491565b840191505092915050565b5f60a0820190508181035f8301526104f181886104a1565b9050818103602083015261050581876104a1565b9050818103604083015261051981866104a1565b9050818103606083015261052d81856104a1565b9050818103608083015261054181846104a1565b9050969550505050505056fea26469706673582212208569967e58690162d7d6fe3513d07b393b4c15e70f41505cbbfd08f53eba739364736f6c63430008190033"),
			// 	Nonce:   0,
			// 	Balance: big.NewInt(0),
			// },
		},
		ExtraData:  []byte("test genesis"),
		Timestamp:  9000,
		BaseFee:    big.NewInt(params.InitialBaseFee),
		Difficulty: big.NewInt(0),
	}
	testNonce := uint64(0)
	generate := func(_ int, g *core.BlockGen) {
		g.OffsetTime(5)
		g.SetExtra([]byte("test"))
		tx, _ := types.SignTx(types.NewTransaction(testNonce, common.HexToAddress("0x9a9070028361F7AAbeB3f2F2Dc07F82C4a98A02a"), big.NewInt(1), params.TxGas, big.NewInt(params.InitialBaseFee*2), nil), types.LatestSigner(&config), testKey)
		g.AddTx(tx)
		testNonce++
	}
	_, blocks, _ := core.GenerateChainWithGenesis(genesis, engine, n, generate)

	if !merged {
		totalDifficulty := big.NewInt(0)
		for _, b := range blocks {
			totalDifficulty.Add(totalDifficulty, b.Difficulty())
		}
		config.TerminalTotalDifficulty = totalDifficulty
	}

	return genesis, blocks
}
