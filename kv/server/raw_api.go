package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	_reader, _error := server.storage.Reader(req.Context)
	if _error != nil {
		return &kvrpcpb.RawGetResponse{}, _error
	}

	val, _error := _reader.GetCF(req.Cf, req.Key)

	if _error != nil {
		return &kvrpcpb.RawGetResponse{}, _error
	}

	_response := &kvrpcpb.RawGetResponse{
		Value: val,
	}

	if val == nil {
		_response.NotFound = true
	} else {
		_response.NotFound = false
	}

	return _response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	_put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}

	_modify := storage.Modify{
		Data: _put,
	}

	_error := server.storage.Write(req.Context, []storage.Modify{_modify})

	_response := &kvrpcpb.RawPutResponse{}

	if _error != nil {
		return _response, _error
	}

	return _response, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	_delete := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}

	_modify := storage.Modify{
		Data: _delete,
	}

	_error := server.storage.Write(req.Context, []storage.Modify{_modify})

	_response := &kvrpcpb.RawDeleteResponse{}

	if _error != nil {
		return _response, _error
	}

	return _response, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	_response := &kvrpcpb.RawScanResponse{}
	_reader, _error := server.storage.Reader(req.Context)

	if _error != nil {
		return _response, _error
	}

	_iter := _reader.IterCF(req.Cf)

	var pair []*kvrpcpb.KvPair
	_limit := req.Limit

	//for (i = head[u]; i; i = e[i].nxt)
	for _iter.Seek(req.StartKey); _iter.Valid(); _iter.Next() {
		_item := _iter.Item()
		_val, _ := _item.Value()
		pair = append(pair, &kvrpcpb.KvPair{
			Key:   _item.Key(),
			Value: _val,
		})
		_limit = _limit - 1
		if _limit == 0 {
			break
		}
	}

	_response.Kvs = pair

	return _response, nil
}
