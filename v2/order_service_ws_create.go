package binance

import (
	"encoding/json"
	"time"

	"github.com/adshao/go-binance/v2/common"
	"github.com/adshao/go-binance/v2/common/websocket"
)

// OrderCreateWsService creates order
type OrderCreateWsService struct {
	c          websocket.Client
	ApiKey     string
	SecretKey  string
	KeyType    string
	TimeOffset int64
}

// NewOrderCreateWsService init OrderCreateWsService
func NewOrderCreateWsService(apiKey, secretKey string) (*OrderCreateWsService, error) {
	conn, err := websocket.NewConnection(WsApiInitReadWriteConn, WebsocketKeepalive, WebsocketTimeoutReadWriteConnection)
	if err != nil {
		return nil, err
	}

	client, err := websocket.NewClient(conn)
	if err != nil {
		return nil, err
	}

	return &OrderCreateWsService{
		c:         client,
		ApiKey:    apiKey,
		SecretKey: secretKey,
		KeyType:   common.KeyTypeHmac,
	}, nil
}

// OrderCreateWsRequest parameters for 'order.place' websocket API
type OrderCreateWsRequest struct {
	symbol           string
	side             SideType
	orderType        OrderType
	timeInForce      *TimeInForceType
	quantity         string
	price            *string
	newClientOrderID *string
	stopPrice        *string
	newOrderRespType NewOrderRespType
	quoteOrderQty    *string
	trailingDelta    *int64
	icebergQty       *string
	strategyId       *uint64
	strategyType     *uint32
	recvWindow       *uint16
}

// OrderListOCOCreateWsRequest parameters for 'orderList.place.oco' websocket API
// docs: https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/trading-requests#order-lists
type OrderListOCOCreateWsRequest struct {
	symbol            string
	listClientOrderId *string
	side              SideType
	quantity          string
	// Above order
	aboveType          OrderType
	aboveClientOrderId *string
	aboveIcebergQty    *string
	abovePrice         *string
	aboveStopPrice     *string
	aboveTrailingDelta *string
	aboveTimeInForce   *string
	aboveStrategyId    *string
	aboveStrategyType  *string
	// Below order
	belowType          OrderType
	belowClientOrderId *string
	belowIcebergQty    *string
	belowPrice         *string
	belowStopPrice     *string
	belowTrailingDelta *string
	belowTimeInForce   *string
	belowStrategyId    *string
	belowStrategyType  *string

	// Common
	newOrderRespType        NewOrderRespType
	selfTradePreventionMode SelfTradePreventionMode
	recvWindow              *uint16
}

func (s *OrderListOCOCreateWsRequest) GetParams() map[string]interface{} {
	return s.buildParams()
}

// buildParams builds params
func (s *OrderListOCOCreateWsRequest) buildParams() params {
	m := params{
		"symbol":           s.symbol,
		"side":             s.side,
		"quantity":         s.quantity,
		"newOrderRespType": s.newOrderRespType,
	}

	if s.listClientOrderId != nil {
		m["listClientOrderId"] = *s.listClientOrderId
	} else {
		m["listClientOrderId"] = common.GenerateSpotId()
	}

	// Above order parameters
	if s.aboveType != "" {
		m["aboveType"] = s.aboveType
	}
	if s.aboveClientOrderId != nil {
		m["aboveClientOrderId"] = *s.aboveClientOrderId
	}
	if s.aboveIcebergQty != nil {
		m["aboveIcebergQty"] = *s.aboveIcebergQty
	}
	if s.abovePrice != nil {
		m["abovePrice"] = *s.abovePrice
	}
	if s.aboveStopPrice != nil {
		m["aboveStopPrice"] = *s.aboveStopPrice
	}
	if s.aboveTrailingDelta != nil {
		m["aboveTrailingDelta"] = *s.aboveTrailingDelta
	}
	if s.aboveTimeInForce != nil {
		m["aboveTimeInForce"] = *s.aboveTimeInForce
	}
	if s.aboveStrategyId != nil {
		m["aboveStrategyId"] = *s.aboveStrategyId
	}
	if s.aboveStrategyType != nil {
		m["aboveStrategyType"] = *s.aboveStrategyType
	}

	// Below order parameters
	if s.belowType != "" {
		m["belowType"] = s.belowType
	}
	if s.belowClientOrderId != nil {
		m["belowClientOrderId"] = *s.belowClientOrderId
	}
	if s.belowIcebergQty != nil {
		m["belowIcebergQty"] = *s.belowIcebergQty
	}
	if s.belowPrice != nil {
		m["belowPrice"] = *s.belowPrice
	}
	if s.belowStopPrice != nil {
		m["belowStopPrice"] = *s.belowStopPrice
	}
	if s.belowTrailingDelta != nil {
		m["belowTrailingDelta"] = *s.belowTrailingDelta
	}
	if s.belowTimeInForce != nil {
		m["belowTimeInForce"] = *s.belowTimeInForce
	}
	if s.belowStrategyId != nil {
		m["belowStrategyId"] = *s.belowStrategyId
	}
	if s.belowStrategyType != nil {
		m["belowStrategyType"] = *s.belowStrategyType
	}

	// Common parameters
	if s.selfTradePreventionMode != "" {
		m["selfTradePreventionMode"] = s.selfTradePreventionMode
	}
	if s.recvWindow != nil {
		m["recvWindow"] = *s.recvWindow
	}

	return m
}

// NewOrderCreateWsRequest init OrderCreateWsRequest
func NewOrderCreateWsRequest() *OrderCreateWsRequest {
	return &OrderCreateWsRequest{}
}

// NewOrderListOCOCreateWsRequest init OrderListOCOCreateWsRequest
func NewOrderListOCOCreateWsRequest() *OrderListOCOCreateWsRequest {
	return &OrderListOCOCreateWsRequest{}
}

func (s *OrderCreateWsRequest) GetParams() map[string]interface{} {
	return s.buildParams()
}

// buildParams builds params
func (s *OrderCreateWsRequest) buildParams() params {
	m := params{
		"symbol":           s.symbol,
		"side":             s.side,
		"type":             s.orderType,
		"newOrderRespType": s.newOrderRespType,
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
	if s.recvWindow != nil {
		m["recvWindow"] = *s.recvWindow
	}
	return m
}

// Do - sends 'order.place' request
func (s *OrderCreateWsService) Do(requestID string, request *OrderCreateWsRequest) error {
	rawData, err := websocket.CreateRequest(
		websocket.NewRequestData(
			requestID,
			s.ApiKey,
			s.SecretKey,
			s.TimeOffset,
			s.KeyType,
		),
		websocket.OrderPlaceSpotWsApiMethod,
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

// SyncDo - sends 'order.place' request and receives response
func (s *OrderCreateWsService) SyncDo(requestID string, request *OrderCreateWsRequest) (*CreateOrderWsResponse, error) {
	rawData, err := websocket.CreateRequest(
		websocket.NewRequestData(
			requestID,
			s.ApiKey,
			s.SecretKey,
			s.TimeOffset,
			s.KeyType,
		),
		websocket.OrderPlaceSpotWsApiMethod,
		request.buildParams(),
	)
	if err != nil {
		return nil, err
	}

	response, err := s.c.WriteSync(requestID, rawData, websocket.WriteSyncWsTimeout)
	if err != nil {
		return nil, err
	}

	createOrderWsResponse := &CreateOrderWsResponse{}
	if err := json.Unmarshal(response, createOrderWsResponse); err != nil {
		return nil, err
	}

	return createOrderWsResponse, nil
}

// SyncOrderListDo - sends 'orderList.place.oco' request and receives response
func (s *OrderCreateWsService) SyncOrderListDo(requestID string, request *OrderListOCOCreateWsRequest) (*CreateOrderListWsResponse, error) {
	rawData, err := websocket.CreateRequest(
		websocket.NewRequestData(
			requestID,
			s.ApiKey,
			s.SecretKey,
			s.TimeOffset,
			s.KeyType,
		),
		websocket.OrderListPlaceOCOSpotWsApiMethod,
		request.GetParams(),
	)
	if err != nil {
		return nil, err
	}

	response, err := s.c.WriteSync(requestID, rawData, websocket.WriteSyncWsTimeout)
	if err != nil {
		return nil, err
	}

	createOrderWsResponse := &CreateOrderListWsResponse{}
	if err := json.Unmarshal(response, createOrderWsResponse); err != nil {
		return nil, err
	}

	return createOrderWsResponse, nil
}

// ReceiveAllDataBeforeStop waits until all responses will be received from websocket until timeout expired
func (s *OrderCreateWsService) ReceiveAllDataBeforeStop(timeout time.Duration) {
	s.c.Wait(timeout)
}

// GetReadChannel returns channel with API response data (including API errors)
func (s *OrderCreateWsService) GetReadChannel() <-chan []byte {
	return s.c.GetReadChannel()
}

// GetReadErrorChannel returns channel with errors which are occurred while reading websocket connection
func (s *OrderCreateWsService) GetReadErrorChannel() <-chan error {
	return s.c.GetReadErrorChannel()
}

// GetReconnectCount returns count of reconnect attempts by client
func (s *OrderCreateWsService) GetReconnectCount() int64 {
	return s.c.GetReconnectCount()
}

// Symbol set symbol
func (s *OrderCreateWsRequest) Symbol(symbol string) *OrderCreateWsRequest {
	s.symbol = symbol
	return s
}

// Side set side
func (s *OrderCreateWsRequest) Side(side SideType) *OrderCreateWsRequest {
	s.side = side
	return s
}

// Type set type
func (s *OrderCreateWsRequest) Type(orderType OrderType) *OrderCreateWsRequest {
	s.orderType = orderType
	return s
}

// TimeInForce set timeInForce
func (s *OrderCreateWsRequest) TimeInForce(timeInForce TimeInForceType) *OrderCreateWsRequest {
	s.timeInForce = &timeInForce
	return s
}

// Quantity set quantity
func (s *OrderCreateWsRequest) Quantity(quantity string) *OrderCreateWsRequest {
	s.quantity = quantity
	return s
}

// Price set price
func (s *OrderCreateWsRequest) Price(price string) *OrderCreateWsRequest {
	s.price = &price
	return s
}

// NewClientOrderID set newClientOrderID
func (s *OrderCreateWsRequest) NewClientOrderID(newClientOrderID string) *OrderCreateWsRequest {
	s.newClientOrderID = &newClientOrderID
	return s
}

// StopPrice set stopPrice
func (s *OrderCreateWsRequest) StopPrice(stopPrice string) *OrderCreateWsRequest {
	s.stopPrice = &stopPrice
	return s
}

// RecvWindow set recvWindow
func (s *OrderCreateWsRequest) RecvWindow(recvWindow uint16) *OrderCreateWsRequest {
	s.recvWindow = &recvWindow
	return s
}

// StrategyType set strategyType
func (s *OrderCreateWsRequest) StrategyType(strategyType uint32) *OrderCreateWsRequest {
	s.strategyType = &strategyType
	return s
}

// StrategyId set strategyId
func (s *OrderCreateWsRequest) StrategyId(strategyId uint64) *OrderCreateWsRequest {
	s.strategyId = &strategyId
	return s
}

// IcebergQty set icebergQty
func (s *OrderCreateWsRequest) IcebergQty(icebergQty string) *OrderCreateWsRequest {
	s.icebergQty = &icebergQty
	return s
}

// TrailingDelta set trailingDelta
func (s *OrderCreateWsRequest) TrailingDelta(trailingDelta int64) *OrderCreateWsRequest {
	s.trailingDelta = &trailingDelta
	return s
}

// QuoteOrderQty set quoteOrderQty
func (s *OrderCreateWsRequest) QuoteOrderQty(quoteOrderQty string) *OrderCreateWsRequest {
	s.quoteOrderQty = &quoteOrderQty
	return s
}

// NewOrderRespType set newOrderRespType
func (s *OrderCreateWsRequest) NewOrderRespType(newOrderRespType NewOrderRespType) *OrderCreateWsRequest {
	s.newOrderRespType = newOrderRespType
	return s
}

// Symbol set symbol
func (s *OrderListOCOCreateWsRequest) Symbol(symbol string) *OrderListOCOCreateWsRequest {
	s.symbol = symbol
	return s
}

// ListClientOrderId set listClientOrderId
func (s *OrderListOCOCreateWsRequest) ListClientOrderId(listClientOrderId string) *OrderListOCOCreateWsRequest {
	s.listClientOrderId = &listClientOrderId
	return s
}

// Side set side
func (s *OrderListOCOCreateWsRequest) Side(side SideType) *OrderListOCOCreateWsRequest {
	s.side = side
	return s
}

// Quantity set quantity
func (s *OrderListOCOCreateWsRequest) Quantity(quantity string) *OrderListOCOCreateWsRequest {
	s.quantity = quantity
	return s
}

// AboveType set aboveType
func (s *OrderListOCOCreateWsRequest) AboveType(aboveType OrderType) *OrderListOCOCreateWsRequest {
	s.aboveType = aboveType
	return s
}

// AboveClientOrderId set aboveClientOrderId
func (s *OrderListOCOCreateWsRequest) AboveClientOrderId(aboveClientOrderId string) *OrderListOCOCreateWsRequest {
	s.aboveClientOrderId = &aboveClientOrderId
	return s
}

// AboveIcebergQty set aboveIcebergQty
func (s *OrderListOCOCreateWsRequest) AboveIcebergQty(aboveIcebergQty string) *OrderListOCOCreateWsRequest {
	s.aboveIcebergQty = &aboveIcebergQty
	return s
}

// AbovePrice set abovePrice
func (s *OrderListOCOCreateWsRequest) AbovePrice(abovePrice string) *OrderListOCOCreateWsRequest {
	s.abovePrice = &abovePrice
	return s
}

// AboveStopPrice set aboveStopPrice
func (s *OrderListOCOCreateWsRequest) AboveStopPrice(aboveStopPrice string) *OrderListOCOCreateWsRequest {
	s.aboveStopPrice = &aboveStopPrice
	return s
}

// AboveTrailingDelta set aboveTrailingDelta
func (s *OrderListOCOCreateWsRequest) AboveTrailingDelta(aboveTrailingDelta string) *OrderListOCOCreateWsRequest {
	s.aboveTrailingDelta = &aboveTrailingDelta
	return s
}

// AboveTimeInForce set aboveTimeInForce
func (s *OrderListOCOCreateWsRequest) AboveTimeInForce(aboveTimeInForce string) *OrderListOCOCreateWsRequest {
	s.aboveTimeInForce = &aboveTimeInForce
	return s
}

// AboveStrategyId set aboveStrategyId
func (s *OrderListOCOCreateWsRequest) AboveStrategyId(aboveStrategyId string) *OrderListOCOCreateWsRequest {
	s.aboveStrategyId = &aboveStrategyId
	return s
}

// AboveStrategyType set aboveStrategyType
func (s *OrderListOCOCreateWsRequest) AboveStrategyType(aboveStrategyType string) *OrderListOCOCreateWsRequest {
	s.aboveStrategyType = &aboveStrategyType
	return s
}

// BelowType set belowType
func (s *OrderListOCOCreateWsRequest) BelowType(belowType OrderType) *OrderListOCOCreateWsRequest {
	s.belowType = belowType
	return s
}

// BelowClientOrderId set belowClientOrderId
func (s *OrderListOCOCreateWsRequest) BelowClientOrderId(belowClientOrderId string) *OrderListOCOCreateWsRequest {
	s.belowClientOrderId = &belowClientOrderId
	return s
}

// BelowIcebergQty set belowIcebergQty
func (s *OrderListOCOCreateWsRequest) BelowIcebergQty(belowIcebergQty string) *OrderListOCOCreateWsRequest {
	s.belowIcebergQty = &belowIcebergQty
	return s
}

// BelowPrice set belowPrice
func (s *OrderListOCOCreateWsRequest) BelowPrice(belowPrice string) *OrderListOCOCreateWsRequest {
	s.belowPrice = &belowPrice
	return s
}

// BelowStopPrice set belowStopPrice
func (s *OrderListOCOCreateWsRequest) BelowStopPrice(belowStopPrice string) *OrderListOCOCreateWsRequest {
	s.belowStopPrice = &belowStopPrice
	return s
}

// BelowTrailingDelta set belowTrailingDelta
func (s *OrderListOCOCreateWsRequest) BelowTrailingDelta(belowTrailingDelta string) *OrderListOCOCreateWsRequest {
	s.belowTrailingDelta = &belowTrailingDelta
	return s
}

// BelowTimeInForce set belowTimeInForce
func (s *OrderListOCOCreateWsRequest) BelowTimeInForce(belowTimeInForce string) *OrderListOCOCreateWsRequest {
	s.belowTimeInForce = &belowTimeInForce
	return s
}

// BelowStrategyId set belowStrategyId
func (s *OrderListOCOCreateWsRequest) BelowStrategyId(belowStrategyId string) *OrderListOCOCreateWsRequest {
	s.belowStrategyId = &belowStrategyId
	return s
}

// BelowStrategyType set belowStrategyType
func (s *OrderListOCOCreateWsRequest) BelowStrategyType(belowStrategyType string) *OrderListOCOCreateWsRequest {
	s.belowStrategyType = &belowStrategyType
	return s
}

// NewOrderRespType set newOrderRespType
func (s *OrderListOCOCreateWsRequest) NewOrderRespType(newOrderRespType NewOrderRespType) *OrderListOCOCreateWsRequest {
	s.newOrderRespType = newOrderRespType
	return s
}

// SelfTradePreventionMode set selfTradePreventionMode
func (s *OrderListOCOCreateWsRequest) SelfTradePreventionMode(selfTradePreventionMode SelfTradePreventionMode) *OrderListOCOCreateWsRequest {
	s.selfTradePreventionMode = selfTradePreventionMode
	return s
}

// RecvWindow set recvWindow
func (s *OrderListOCOCreateWsRequest) RecvWindow(recvWindow uint16) *OrderListOCOCreateWsRequest {
	s.recvWindow = &recvWindow
	return s
}

// CreateOrderResult define order creation result
type CreateOrderResult struct {
	CreateOrderResponse
}

// CreateOrderWsResponse define 'order.place' websocket API response
type CreateOrderWsResponse struct {
	Id     string            `json:"id"`
	Status int               `json:"status"`
	Result CreateOrderResult `json:"result"`

	// error response
	Error *common.APIError `json:"error,omitempty"`
}

// CreateOrderListWsResponse define 'orderList.place.oco' websocket API response
type CreateOrderListWsResponse struct {
	Id     string                `json:"id"`
	Status int                   `json:"status"`
	Result CreateOrderListResult `json:"result"`

	// error response
	Error      *common.APIError `json:"error,omitempty"`
	RateLimits []RateLimitInfo  `json:"rateLimits"`
}

// CreateOrderListResult define order list creation result
type CreateOrderListResult struct {
	OrderListId       int64             `json:"orderListId"`
	ContingencyType   string            `json:"contingencyType"`
	ListStatusType    string            `json:"listStatusType"`
	ListOrderStatus   string            `json:"listOrderStatus"`
	ListClientOrderId string            `json:"listClientOrderId"`
	TransactionTime   int64             `json:"transactionTime"`
	Symbol            string            `json:"symbol"`
	Orders            []*OCOOrder       `json:"orders"`
	OrderReports      []*OCOOrderReport `json:"orderReports"`
}

// RateLimitInfo define rate limit info
type RateLimitInfo struct {
	RateLimitType string `json:"rateLimitType"`
	Interval      string `json:"interval"`
	IntervalNum   int64  `json:"intervalNum"`
	Limit         int64  `json:"limit"`
	Count         int64  `json:"count"`
}
