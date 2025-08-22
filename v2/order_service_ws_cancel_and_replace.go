package binance

import (
	"encoding/json"
	"time"

	"github.com/adshao/go-binance/v2/common"
	"github.com/adshao/go-binance/v2/common/websocket"
)

// OrderCancelAndReplaceWsService cancels and replaces an order
type OrderCancelAndReplaceWsService struct {
	c          websocket.Client
	ApiKey     string
	SecretKey  string
	KeyType    string
	TimeOffset int64
}

// NewOrderCancelAndReplaceWsService init OrderCancelAndReplaceWsService
func NewOrderCancelAndReplaceWsService(apiKey, secretKey string) (*OrderCancelAndReplaceWsService, error) {
	conn, err := websocket.NewConnection(WsApiInitReadWriteConn, WebsocketKeepalive, WebsocketTimeoutReadWriteConnection)
	if err != nil {
		return nil, err
	}

	client, err := websocket.NewClient(conn)
	if err != nil {
		return nil, err
	}

	return &OrderCancelAndReplaceWsService{
		c:         client,
		ApiKey:    apiKey,
		SecretKey: secretKey,
		KeyType:   common.KeyTypeHmac,
	}, nil
}

// OrderCancelAndReplaceWsRequest parameters for 'order.cancelReplace' websocket API
type OrderCancelAndReplaceWsRequest struct {
	symbol                     string
	cancelReplaceMode          CancelReplaceMode
	cancelOrderId              *string
	cancelOrigClientOrderId    *string
	cancelNewClientOrderId     *string
	side                       SideType
	orderType                  OrderType
	timeInForce                *TimeInForceType
	quantity                   string
	price                      *string
	newClientOrderID           *string
	stopPrice                  *string
	newOrderRespType           NewOrderRespType
	quoteOrderQty              *string
	trailingDelta              *int64
	icebergQty                 *string
	strategyId                 *uint64
	strategyType               *uint32
	selfTradePreventionMode    SelfTradePreventionMode
	cancelRestrictions         CancelRestrictionsType
	orderRateLimitExceededMode OrderRateLimitExceededMode
	recvWindow                 *uint16
}

// NewOrderCancelAndReplaceWsRequest init OrderCancelAndReplaceWsRequest
func NewOrderCancelAndReplaceWsRequest() *OrderCancelAndReplaceWsRequest {
	return &OrderCancelAndReplaceWsRequest{}
}

func (s *OrderCancelAndReplaceWsRequest) GetParams() map[string]interface{} {
	return s.buildParams()
}

// buildParams builds params
func (s *OrderCancelAndReplaceWsRequest) buildParams() params {
	m := params{
		"symbol":            s.symbol,
		"cancelReplaceMode": s.cancelReplaceMode,
		"side":              s.side,
		"type":              s.orderType,
		"newOrderRespType":  s.newOrderRespType,
	}

	if s.cancelOrderId != nil {
		m["cancelOrderId"] = *s.cancelOrderId
	}
	if s.cancelOrigClientOrderId != nil {
		m["cancelOrigClientOrderId"] = *s.cancelOrigClientOrderId
	}
	if s.cancelNewClientOrderId != nil {
		m["cancelNewClientOrderId"] = *s.cancelNewClientOrderId
	}
	if s.quantity != "" {
		m["quantity"] = s.quantity
	}
	if s.timeInForce != nil {
		m["timeInForce"] = *s.timeInForce
	}
	if s.price != nil {
		m["price"] = *s.price
	}
	if s.newClientOrderID != nil {
		m["newClientOrderId"] = *s.newClientOrderID
	} else {
		m["newClientOrderId"] = common.GenerateSpotId()
	}
	if s.stopPrice != nil {
		m["stopPrice"] = *s.stopPrice
	}
	if s.quoteOrderQty != nil {
		m["quoteOrderQty"] = *s.quoteOrderQty
	}
	if s.trailingDelta != nil {
		m["trailingDelta"] = *s.trailingDelta
	}
	if s.icebergQty != nil {
		m["icebergQty"] = *s.icebergQty
	}
	if s.strategyId != nil {
		m["strategyId"] = *s.strategyId
	}
	if s.strategyType != nil {
		m["strategyType"] = *s.strategyType
	}

	if s.selfTradePreventionMode != "" {
		m["selfTradePreventionMode"] = s.selfTradePreventionMode
	}
	if s.cancelRestrictions != "" {
		m["cancelRestrictions"] = s.cancelRestrictions
	}
	if s.orderRateLimitExceededMode != "" {
		m["orderRateLimitExceededMode"] = s.orderRateLimitExceededMode
	}
	if s.recvWindow != nil {
		m["recvWindow"] = *s.recvWindow
	}
	return m
}

// Do - sends 'order.cancelReplace' request
func (s *OrderCancelAndReplaceWsService) Do(requestID string, request *OrderCancelAndReplaceWsRequest) error {
	rawData, err := websocket.CreateRequest(
		websocket.NewRequestData(
			requestID,
			s.ApiKey,
			s.SecretKey,
			s.TimeOffset,
			s.KeyType,
		),
		websocket.OrderCancelAndReplaceWsApiMethod,
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

// SyncDo - sends 'order.cancelReplace' request and receives response
func (s *OrderCancelAndReplaceWsService) SyncDo(requestID string, request *OrderCancelAndReplaceWsRequest) (*CancelAndReplaceWsResponse, error) {
	rawData, err := websocket.CreateRequest(
		websocket.NewRequestData(
			requestID,
			s.ApiKey,
			s.SecretKey,
			s.TimeOffset,
			s.KeyType,
		),
		websocket.OrderCancelAndReplaceWsApiMethod,
		request.buildParams(),
	)
	if err != nil {
		return nil, err
	}

	response, err := s.c.WriteSync(requestID, rawData, websocket.WriteSyncWsTimeout)
	if err != nil {
		return nil, err
	}

	cancelAndReplaceOrderWsResponse := &CancelAndReplaceWsResponse{}
	if err := json.Unmarshal(response, cancelAndReplaceOrderWsResponse); err != nil {
		return nil, err
	}

	return cancelAndReplaceOrderWsResponse, nil
}

// ReceiveAllDataBeforeStop waits until all responses will be received from websocket until timeout expired
func (s *OrderCancelAndReplaceWsService) ReceiveAllDataBeforeStop(timeout time.Duration) {
	s.c.Wait(timeout)
}

// GetReadChannel returns channel with API response data (including API errors)
func (s *OrderCancelAndReplaceWsService) GetReadChannel() <-chan []byte {
	return s.c.GetReadChannel()
}

// GetReadErrorChannel returns channel with errors which are occurred while reading websocket connection
func (s *OrderCancelAndReplaceWsService) GetReadErrorChannel() <-chan error {
	return s.c.GetReadErrorChannel()
}

// GetReconnectCount returns count of reconnect attempts by client
func (s *OrderCancelAndReplaceWsService) GetReconnectCount() int64 {
	return s.c.GetReconnectCount()
}

// Symbol set symbol
func (s *OrderCancelAndReplaceWsRequest) Symbol(symbol string) *OrderCancelAndReplaceWsRequest {
	s.symbol = symbol
	return s
}

// CancelReplaceMode set cancelReplaceMode
func (s *OrderCancelAndReplaceWsRequest) CancelReplaceMode(cancelReplaceMode CancelReplaceMode) *OrderCancelAndReplaceWsRequest {
	s.cancelReplaceMode = cancelReplaceMode
	return s
}

// CancelOrderId set cancelOrderId
func (s *OrderCancelAndReplaceWsRequest) CancelOrderId(cancelOrderId string) *OrderCancelAndReplaceWsRequest {
	s.cancelOrderId = &cancelOrderId
	return s
}

// CancelOrigClientOrderId set cancelOrigClientOrderId
func (s *OrderCancelAndReplaceWsRequest) CancelOrigClientOrderId(cancelOrigClientOrderId string) *OrderCancelAndReplaceWsRequest {
	s.cancelOrigClientOrderId = &cancelOrigClientOrderId
	return s
}

// CancelNewClientOrderId set cancelNewClientOrderId
func (s *OrderCancelAndReplaceWsRequest) CancelNewClientOrderId(cancelNewClientOrderId string) *OrderCancelAndReplaceWsRequest {
	s.cancelNewClientOrderId = &cancelNewClientOrderId
	return s
}

// Side set side
func (s *OrderCancelAndReplaceWsRequest) Side(side SideType) *OrderCancelAndReplaceWsRequest {
	s.side = side
	return s
}

// Type set type
func (s *OrderCancelAndReplaceWsRequest) Type(orderType OrderType) *OrderCancelAndReplaceWsRequest {
	s.orderType = orderType
	return s
}

// TimeInForce set timeInForce
func (s *OrderCancelAndReplaceWsRequest) TimeInForce(timeInForce TimeInForceType) *OrderCancelAndReplaceWsRequest {
	s.timeInForce = &timeInForce
	return s
}

// Quantity set quantity
func (s *OrderCancelAndReplaceWsRequest) Quantity(quantity string) *OrderCancelAndReplaceWsRequest {
	s.quantity = quantity
	return s
}

// Price set price
func (s *OrderCancelAndReplaceWsRequest) Price(price string) *OrderCancelAndReplaceWsRequest {
	s.price = &price
	return s
}

// NewClientOrderID set newClientOrderID
func (s *OrderCancelAndReplaceWsRequest) NewClientOrderID(newClientOrderID string) *OrderCancelAndReplaceWsRequest {
	s.newClientOrderID = &newClientOrderID
	return s
}

// StopPrice set stopPrice
func (s *OrderCancelAndReplaceWsRequest) StopPrice(stopPrice string) *OrderCancelAndReplaceWsRequest {
	s.stopPrice = &stopPrice
	return s
}

// RecvWindow set recvWindow
func (s *OrderCancelAndReplaceWsRequest) RecvWindow(recvWindow uint16) *OrderCancelAndReplaceWsRequest {
	s.recvWindow = &recvWindow
	return s
}

// StrategyType set strategyType
func (s *OrderCancelAndReplaceWsRequest) StrategyType(strategyType uint32) *OrderCancelAndReplaceWsRequest {
	s.strategyType = &strategyType
	return s
}

// StrategyId set strategyId
func (s *OrderCancelAndReplaceWsRequest) StrategyId(strategyId uint64) *OrderCancelAndReplaceWsRequest {
	s.strategyId = &strategyId
	return s
}

// SelfTradePreventionMode set selfTradePreventionMode
func (s *OrderCancelAndReplaceWsRequest) SelfTradePreventionMode(selfTradePreventionMode SelfTradePreventionMode) *OrderCancelAndReplaceWsRequest {
	s.selfTradePreventionMode = selfTradePreventionMode
	return s
}

// CancelRestrictions set cancelRestrictions
func (s *OrderCancelAndReplaceWsRequest) CancelRestrictions(cancelRestrictions CancelRestrictionsType) *OrderCancelAndReplaceWsRequest {
	s.cancelRestrictions = cancelRestrictions
	return s
}

// OrderRateLimitExceededMode set orderRateLimitExceededMode
func (s *OrderCancelAndReplaceWsRequest) OrderRateLimitExceededMode(orderRateLimitExceededMode OrderRateLimitExceededMode) *OrderCancelAndReplaceWsRequest {
	s.orderRateLimitExceededMode = orderRateLimitExceededMode
	return s
}

// IcebergQty set icebergQty
func (s *OrderCancelAndReplaceWsRequest) IcebergQty(icebergQty string) *OrderCancelAndReplaceWsRequest {
	s.icebergQty = &icebergQty
	return s
}

// TrailingDelta set trailingDelta
func (s *OrderCancelAndReplaceWsRequest) TrailingDelta(trailingDelta int64) *OrderCancelAndReplaceWsRequest {
	s.trailingDelta = &trailingDelta
	return s
}

// QuoteOrderQty set quoteOrderQty
func (s *OrderCancelAndReplaceWsRequest) QuoteOrderQty(quoteOrderQty string) *OrderCancelAndReplaceWsRequest {
	s.quoteOrderQty = &quoteOrderQty
	return s
}

// NewOrderRespType set newOrderRespType
func (s *OrderCancelAndReplaceWsRequest) NewOrderRespType(newOrderRespType NewOrderRespType) *OrderCancelAndReplaceWsRequest {
	s.newOrderRespType = newOrderRespType
	return s
}

// CancelAndReplaceOrderResult define order creation result
type CancelAndReplaceOrderResult struct {
	CancelResponse   CreateOrderResponse             `json:"cancelResponse"`
	NewOrderResponse CreateOrderResponse             `json:"newOrderResponse"`
	CancelResult     CancelAndReplaceOrderResultType `json:"cancelResult"`
	NewOrderResult   CancelAndReplaceOrderResultType `json:"newOrderResult"`
}

// CancelAndReplaceWsResponse define 'order.cancelReplace' websocket API response
type CancelAndReplaceWsResponse struct {
	Id     string                      `json:"id"`
	Status int                         `json:"status"`
	Result CancelAndReplaceOrderResult `json:"result"`

	// error response
	Error *common.APIError `json:"error,omitempty"`
}
