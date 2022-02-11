package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	config *config.Config
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	db_Path := conf.DBPath
	kv_Path := path.Join(db_Path, "kv")
	raft_Path := path.Join(db_Path, "raft")

	//构造好path 然后传一个struct回去

	kvEng := engine_util.CreateDB(kv_Path, conf.Raft) //raft_bool 就是当前create的玩意是不是Raft
	raftEng := engine_util.CreateDB(raft_Path, conf.Raft)

	return &StandAloneStorage{
		config: conf,
		engine: engine_util.NewEngines(kvEng, raftEng, kv_Path, raft_Path), //做Project1的话不需要用上raft
	}

}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

type StandAloneReader struct {
	kv_Txn *badger.Txn
}

func NewStandAloneReader(kv_Txn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		kv_Txn: kv_Txn,
	}
	//就是new一个传回去
}

//Reader 应该返回一个 StorageReader，支持 key/value 的单点读取和快照扫描的操作。

//使用 badger.Txn 来实现 Reader 函数，因为 badger 提供的事务处理程序可以提供 keys 和 values 的一致快照

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneReader(s.engine.Kv.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	for _, b := range batch { //用range遍历batch，_代表空变量
		switch b.Data.(type) { //参考modify.go的Line19
		case storage.Put:
			_put := b.Data.(storage.Put)
			_error := engine_util.PutCF(s.engine.Kv, _put.Cf, _put.Key, _put.Value)
			if _error != nil {
				return _error //发生错误
			}
		case storage.Delete:
			_delete := b.Data.(storage.Delete)
			_error := engine_util.DeleteCF(s.engine.Kv, _delete.Cf, _delete.Key)
			if _error != nil {
				return _error //模仿上面
			}
		}
	}
	return nil
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, _error := engine_util.GetCFFromTxn(s.kv_Txn, cf, key)
	if _error == badger.ErrKeyNotFound { //如果没有找到这个key，返回空信息
		return nil, nil
	}
	return val, _error
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.kv_Txn)
}

func (s *StandAloneReader) Close() {
	s.kv_Txn.Discard()
	//这个不要忘记
}
