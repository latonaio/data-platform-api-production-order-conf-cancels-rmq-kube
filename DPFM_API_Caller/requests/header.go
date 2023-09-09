package requests

type Header struct {
	ProductionOrder                  int      `json:"ProductionOrder"`
	ProductionOrderItem              int      `json:"ProductionOrderItem"`
	Operations                       int      `json:"Operations"`
	OperationsItem                   int      `json:"OperationsItem"`
	OperationID                      int      `json:"OperationID"`
	ConfirmationCountingID           int      `json:"ConfirmationCountingID"`
	IsCancelled		                *bool     `json:"IsCancelled"`
}
