package binance

import (
	"encoding/json"
	"time"

	"github.com/adshao/go-binance/v2/common"
	"github.com/adshao/go-binance/v2/common/websocket"
)

// NewOrderCancelRequest init OrderCancelRequest
func NewOrderCancelRequest() *OrderCancelRequest {
	return &OrderCancelRequest{}
}

// OrderCancelRequest parameters for 'order.cancel' websocket API
type OrderCancelRequest struct {
	symbol             string
	orderID            *int64
	origClientOrderID  *string
	newClientOrderID   *string
	cancelRestrictions *CancelRestrictionsType
}

// Symbol set symbol
func (r *OrderCancelRequest) Symbol(symbol string) *OrderCancelRequest {
	r.symbol = symbol
	return r
}

// OrderID set orderID
func (r *OrderCancelRequest) OrderID(orderID int64) *OrderCancelRequest {
	r.orderID = &orderID
	return r
}

// NewClientOrderID set newClientOrderID
func (r *OrderCancelRequest) NewClientOrderID(newClientOrderId string) *OrderCancelRequest {
	r.newClientOrderID = &newClientOrderId
	return r
}

// CancelRestrictions set cancelRestrictions
func (r *OrderCancelRequest) CancelRestrictions(cancelRestrictions CancelRestrictionsType) *OrderCancelRequest {
	r.cancelRestrictions = &cancelRestrictions
	return r
}

// OrigClientOrderID set origClientOrderID
func (r *OrderCancelRequest) OrigClientOrderID(origClientOrderID string) *OrderCancelRequest {
	r.origClientOrderID = &origClientOrderID
	return r
}

func (r *OrderCancelRequest) GetParams() map[string]interface{} {
	return r.buildParams()
}

// buildParams builds params
func (r *OrderCancelRequest) buildParams() params {
	m := params{
		"symbol": r.symbol,
	}

	if r.orderID != nil {
		m["orderId"] = *r.orderID
	}

	if r.origClientOrderID != nil {
		m["origClientOrderId"] = *r.origClientOrderID
	}

	if r.newClientOrderID != nil {
		m["newClientOrderId"] = *r.newClientOrderID
	}

	if r.cancelRestrictions != nil {
		m["cancelRestrictions"] = *r.cancelRestrictions
	}
	return m
}

// CancelOrderResult define order cancel result
type CancelOrderResult struct {
	CancelOrderResponse
}

// OrderCancelWsResponse define 'order.cancel' websocket API response
type OrderCancelWsResponse struct {
	Id     string            `json:"id"`
	Status int               `json:"status"`
	Result CancelOrderResult `json:"result"`

	// error response
	Error *common.APIError `json:"error,omitempty"`
}

// OrderCancelWsService cancel order
type OrderCancelWsService struct {
	c          websocket.Client
	ApiKey     string
	SecretKey  string
	KeyType    string
	TimeOffset int64
}

// NewOrderCancelWsService init OrderCancelWsService
func NewOrderCancelWsService(apiKey, secretKey string) (*OrderCancelWsService, error) {
	conn, err := websocket.NewConnection(WsApiInitReadWriteConn, WebsocketKeepalive, WebsocketTimeoutReadWriteConnection)
	if err != nil {
		return nil, err
	}

	client, err := websocket.NewClient(conn)
	if err != nil {
		return nil, err
	}

	return &OrderCancelWsService{
		c:         client,
		ApiKey:    apiKey,
		SecretKey: secretKey,
		KeyType:   common.KeyTypeHmac,
	}, nil
}

// Do - sends 'order.cancel' request
func (s *OrderCancelWsService) Do(requestID string, request *OrderCancelRequest) error {
	rawData, err := websocket.CreateRequest(
		websocket.NewRequestData(
			requestID,
			s.ApiKey,
			s.SecretKey,
			s.TimeOffset,
			s.KeyType,
		),
		websocket.CancelOrderWsApiMethod,
		request.buildParams(),
	)
	if err != nil {
		return err
	}

	if err := s.c.Write(requestID, rawData); err != nil {
		return err
	}

	return nil
}

// SyncDo - sends 'order.cancel' request and receives response
func (s *OrderCancelWsService) SyncDo(requestID string, request *OrderCancelRequest) (*OrderCancelWsResponse, error) {
	rawData, err := websocket.CreateRequest(
		websocket.NewRequestData(
			requestID,
			s.ApiKey,
			s.SecretKey,
			s.TimeOffset,
			s.KeyType,
		),
		websocket.CancelOrderWsApiMethod,
		request.buildParams(),
	)
	if err != nil {
		return nil, err
	}

	response, err := s.c.WriteSync(requestID, rawData, websocket.WriteSyncWsTimeout)
	if err != nil {
		return nil, err
	}

	cancelOrderWsResponse := &OrderCancelWsResponse{}
	if err := json.Unmarshal(response, cancelOrderWsResponse); err != nil {
		return nil, err
	}

	return cancelOrderWsResponse, nil
}

// ReceiveAllDataBeforeStop waits until all responses will be received from websocket until timeout expired
func (s *OrderCancelWsService) ReceiveAllDataBeforeStop(timeout time.Duration) {
	s.c.Wait(timeout)
}

// GetReadChannel returns channel with API response data (including API errors)
func (s *OrderCancelWsService) GetReadChannel() <-chan []byte {
	return s.c.GetReadChannel()
}

// GetReadErrorChannel returns channel with errors which are occurred while reading websocket connection
func (s *OrderCancelWsService) GetReadErrorChannel() <-chan error {
	return s.c.GetReadErrorChannel()
}

// GetReconnectCount returns count of reconnect attempts by client
func (s *OrderCancelWsService) GetReconnectCount() int64 {
	return s.c.GetReconnectCount()
}
